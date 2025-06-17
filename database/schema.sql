-- Enable pgvector extension for vector operations
CREATE EXTENSION IF NOT EXISTS vector;

-- Main table for video metadata (based on your video_analysis_report structure)
CREATE TABLE IF NOT EXISTS videos (
    video_id VARCHAR(255) PRIMARY KEY,
    original_filename VARCHAR(255) NOT NULL,
    compressed_filename VARCHAR(255),
    video_path VARCHAR(500),
    compressed_video_path VARCHAR(500),
    duration_seconds FLOAT NOT NULL,
    fps FLOAT NOT NULL,
    compressed_file_size_bytes BIGINT,
    processing_date_utc TIMESTAMP,
    scene_change_timestamps FLOAT[],  -- Array of shot boundaries
    keyframes_analyzed_count INTEGER DEFAULT 0,
    analysis_status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for video moments (keyframes) - matches your 'video_moments' table name
CREATE TABLE IF NOT EXISTS video_moments (
    moment_id VARCHAR(512) PRIMARY KEY,  -- Your format: video_id_frame_unique_id
    video_id VARCHAR(255) NOT NULL,
    frame_identifier VARCHAR(255) NOT NULL,  -- Your frame_unique_id
    timestamp_seconds FLOAT NOT NULL,
    
    -- Image storage
    keyframe_image_path VARCHAR(500),  -- Relative path from DATASET_ROOT_DIR
    
    -- CLIP embeddings (768 dimensions for ViT-L/14)
    clip_embedding TEXT,
    
    -- Simple search fields (from your structure)
    detected_object_names TEXT[],  -- Array of object names for quick search
    extracted_search_words TEXT[], -- Array of words for text search
    average_color_rgb INTEGER[3],  -- RGB values as array
    
    -- Detailed features (JSONB for complex search)
    detailed_features JSONB,  -- Your detected_objects_detailed, extracted_text_detailed, etc.
    
    -- Technical metadata
    extraction_success BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(analysis_status);
CREATE INDEX IF NOT EXISTS idx_videos_duration ON videos(duration_seconds);

CREATE INDEX IF NOT EXISTS idx_moments_video_id ON video_moments(video_id);
CREATE INDEX IF NOT EXISTS idx_moments_timestamp ON video_moments(timestamp_seconds);
CREATE INDEX IF NOT EXISTS idx_moments_frame_id ON video_moments(frame_identifier);

-- Vector similarity search index (768 dimensions for your CLIP model)
CREATE INDEX IF NOT EXISTS idx_moments_clip_embedding 
ON video_moments USING ivfflat (clip_embedding vector_cosine_ops) 
WITH (lists = 100);

-- Text search indexes
CREATE INDEX IF NOT EXISTS idx_moments_objects 
ON video_moments USING gin(detected_object_names);

CREATE INDEX IF NOT EXISTS idx_moments_words 
ON video_moments USING gin(extracted_search_words);

-- JSON search index for detailed features
CREATE INDEX IF NOT EXISTS idx_moments_detailed_features 
ON video_moments USING gin(detailed_features);

-- Color search index (for RGB matching)
CREATE INDEX IF NOT EXISTS idx_moments_avg_color 
ON video_moments(average_color_rgb);

-- Function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for automatic timestamp update
CREATE TRIGGER update_videos_updated_at 
BEFORE UPDATE ON videos
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- View for detailed moment information with video context
CREATE VIEW IF NOT EXISTS moments_with_video_info AS
SELECT 
    m.moment_id,
    m.video_id,
    m.frame_identifier,
    m.timestamp_seconds,
    m.keyframe_image_path,
    m.detected_object_names,
    m.extracted_search_words,
    m.average_color_rgb,
    m.detailed_features,
    v.original_filename,
    v.compressed_filename,
    v.duration_seconds,
    v.fps,
    v.analysis_status
FROM video_moments m
JOIN videos v ON m.video_id = v.video_id;

-- Function for vector similarity search
CREATE OR REPLACE FUNCTION search_similar_moments(
    reference_embedding VECTOR(768),
    similarity_threshold FLOAT DEFAULT 0.75,
    result_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    moment_id VARCHAR(512),
    video_id VARCHAR(255),
    timestamp_seconds FLOAT,
    keyframe_image_path VARCHAR(500),
    similarity_score FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.moment_id,
        m.video_id,
        m.timestamp_seconds,
        m.keyframe_image_path,
        (1 - (m.clip_embedding <=> reference_embedding)) as similarity_score
    FROM video_moments m
    WHERE m.clip_embedding IS NOT NULL
    AND (1 - (m.clip_embedding <=> reference_embedding)) >= similarity_threshold
    ORDER BY m.clip_embedding <=> reference_embedding
    LIMIT result_limit;
END;
$$ LANGUAGE plpgsql;

-- Function for color similarity search (based on your RGB matching logic)
CREATE OR REPLACE FUNCTION search_by_color(
    target_rgb INTEGER[],
    color_tolerance INTEGER DEFAULT 25,
    result_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    moment_id VARCHAR(512),
    video_id VARCHAR(255),
    timestamp_seconds FLOAT,
    keyframe_image_path VARCHAR(500),
    avg_color INTEGER[],
    color_distance FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.moment_id,
        m.video_id,
        m.timestamp_seconds,
        m.keyframe_image_path,
        m.average_color_rgb,
        SQRT(
            POWER(m.average_color_rgb[1] - target_rgb[1], 2) +
            POWER(m.average_color_rgb[2] - target_rgb[2], 2) +
            POWER(m.average_color_rgb[3] - target_rgb[3], 2)
        ) as color_distance
    FROM video_moments m
    WHERE m.average_color_rgb IS NOT NULL
    AND ABS(m.average_color_rgb[1] - target_rgb[1]) <= color_tolerance
    AND ABS(m.average_color_rgb[2] - target_rgb[2]) <= color_tolerance
    AND ABS(m.average_color_rgb[3] - target_rgb[3]) <= color_tolerance
    ORDER BY color_distance
    LIMIT result_limit;
END;
$$ LANGUAGE plpgsql;

-- Add comments for documentation
COMMENT ON TABLE videos IS 'Main video metadata table from video_analysis_report JSON';
COMMENT ON TABLE video_moments IS 'Keyframe moments table with AI analysis results';
COMMENT ON COLUMN video_moments.clip_embedding IS '768-dimensional CLIP embeddings from ViT-L/14 model';
COMMENT ON COLUMN video_moments.detailed_features IS 'JSONB containing detected_objects_detailed, extracted_text_detailed, dominant_colors_info';
COMMENT ON COLUMN video_moments.detected_object_names IS 'Array of object names for quick filtering';
COMMENT ON COLUMN video_moments.extracted_search_words IS 'Array of extracted words for text search';

-- Example of initial setup queries you might want to run
-- SELECT 'Database schema created successfully for Video Retrieval System' as status;