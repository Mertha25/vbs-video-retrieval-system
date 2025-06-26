// src/components/ConnectionStatus/ConnectionStatus.tsx
import React from 'react'
import { useConnectionStatus } from '../../hooks/useConnectionStatus'
import styles from './ConnectionStatus.module.css'

// ConnectionStatus: shows backend & DRES status dots
export const ConnectionStatus: React.FC = () => {
  const { backend, dres } = useConnectionStatus()

  const renderDot = (s: { isLoading: boolean; isError: boolean }) => {
    const className = s.isLoading
      ? styles.dotGray
      : s.isError
      ? styles.dotRed
      : styles.dotGreen

    return <span className={`${styles.dot} ${className}`} />
  }

  return (
    <div className={styles.statusRoot}>
      <div className={styles.statusItem}>
        {renderDot(dres)} <span>DRES</span>
      </div>
    </div>
  )
}
