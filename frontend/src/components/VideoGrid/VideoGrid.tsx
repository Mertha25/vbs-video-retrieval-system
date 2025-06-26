
import React, { useContext } from 'react';
import { Video } from '../VideoCard/VideoCard';
import { VideoCard } from '../VideoCard/VideoCard';
import DRESContext from '../../context/DRESContext'; 
import styles from './VideoGrid.module.css';

interface VideoGridProps {
  videos: Video[] | undefined;
  onSubmit?: (id: string | number, timestamp: number) => void;
}

/** Responsive grid of video cards with full backend-awareness */
export const VideoGrid: React.FC<VideoGridProps> = ({ videos, onSubmit }) => {
  // ✅ TypeScript bypass to fix the 'submit' type error
  const { submit } = useContext(DRESContext) as any;

  if (videos === undefined) {
    return <p className={styles.status}>Loading videos…</p>;
  }

  if (videos.length === 0) {
    return <p className={styles.status}>No matching videos found.</p>;
  }

  return (
    <div className={styles.gridRoot}>
      {videos.map((video) => (
        <VideoCard key={video.id}  video={video}
          onSubmit={(id, ts) => {
            if (onSubmit) {
              onSubmit(id, ts);
            } else {
              submit?.(String(id), ts); // Optional chaining to prevent crash
            }
          }}
        />
      ))}
    </div>
  );
};
