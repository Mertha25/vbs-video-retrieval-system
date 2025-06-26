// SkeletonGrid.tsx
import React from 'react'
import styles from './SkeletonGrid.module.css'

export const SkeletonGrid: React.FC = () => (
  <div className={styles.gridRoot}>
    {Array.from({ length: 6 }).map((_, i) => (
      <div key={i} className={styles.skeletonCard}>
        <div className={styles.skeletonVideo} />
        <div className={styles.skeletonTitle} />
      </div>
    ))}
  </div>
)
