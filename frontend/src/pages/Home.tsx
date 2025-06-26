// frontend/src/pages/Home.tsx
import React, { useState } from 'react';
import { FilterPanel, FilterCriteria } from '../components/FilterPanel/FilterPanel';
import { VideoGrid } from '../components/VideoGrid/VideoGrid';
import { useSearchVideos } from '../hooks/useSearchVideos';
import { mockVideos } from '../data/mockVideos';
import styles from './Home.module.css';

export const Home: React.FC = () => {
  const [criteria, setCriteria] = useState<FilterCriteria>({});
  const { results, loading, error, extractedKeywords, originalQuery, searchWithCriteria } = useSearchVideos();

  // When filter changes, run the search
  const handleFilterChange = (newCriteria: FilterCriteria) => {
    setCriteria(newCriteria);
    searchWithCriteria(newCriteria);
  };

  const videosToShow = !error ? results : mockVideos;

  return (
    <main className={styles.mainRoot}>
      <div className={styles.layout}>
        {/* Sidebar with filters and manual search trigger */}
        <div className={styles.sidebar}>
          <FilterPanel onChange={handleFilterChange} />
        </div>

        {/* Main content with video grid */}
        <div className={styles.content}>
          {loading && <p style={{ padding: '1rem' }}>🔄 Loading results...</p>}
          {error && (
            <p style={{ padding: '1rem', color: 'red' }}>
              ⚠️ {error} – showing mock results instead.
            </p>
          )}
          
          {/* Display extracted keywords when available */}
          {!loading && extractedKeywords.length > 0 && (
            <div style={{ 
              padding: '1rem', 
              marginBottom: '1rem', 
              backgroundColor: '#f0f9ff', 
              border: '1px solid #0ea5e9', 
              borderRadius: '0.5rem',
              fontSize: '0.9rem'
            }}>
              <p style={{ margin: '0 0 0.5rem 0', fontWeight: '600', color: '#0369a1' }}>
                🔍 Search Analysis:
              </p>
              <p style={{ margin: '0 0 0.5rem 0', color: '#374151' }}>
                <strong>Original query:</strong> "{originalQuery}"
              </p>
              <p style={{ margin: '0', color: '#374151' }}>
                <strong>Extracted keywords:</strong> {extractedKeywords.join(', ')}
              </p>
            </div>
          )}
          
          {!loading && videosToShow.length === 0 && (
            <p style={{ padding: '1rem' }}>No results found.</p>
          )}
          <VideoGrid videos={videosToShow} />
        </div>
      </div>
    </main>
  );
};
