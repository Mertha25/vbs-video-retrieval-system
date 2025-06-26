import React, { useEffect, useState } from 'react';
import { useDRES } from '../../hooks/useDRES';
import styles from './DRESStatus.module.css';

export const DRESStatus: React.FC = () => {
  const { status, loading, error, getStatus, isConnected, hasActiveQueries } = useDRES();
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);

  // Auto-refresh DRES status every 30 seconds
  useEffect(() => {
    const fetchStatus = async () => {
      await getStatus();
      setLastUpdate(new Date());
    };

    // Initial fetch
    fetchStatus();

    // Set up interval
    const interval = setInterval(fetchStatus, 30000);

    return () => clearInterval(interval);
  }, [getStatus]);

  const getStatusIcon = () => {
    if (loading) return '🔄';
    if (error) return '❌';
    if (isConnected) return '✅';
    return '⚠️';
  };

  const getStatusText = () => {
    if (loading) return 'Checking DRES...';
    if (error) return 'DRES Error';
    if (isConnected) return 'DRES Connected';
    return 'DRES Disconnected';
  };

  const getStatusClass = () => {
    if (loading) return styles.loading;
    if (error) return styles.error;
    if (isConnected) return styles.connected;
    return styles.disconnected;
  };

  const formatLastUpdate = () => {
    if (!lastUpdate) return '';
    return lastUpdate.toLocaleTimeString();
  };

  return (
    <div className={`${styles.dresStatus} ${getStatusClass()}`}>
      <div className={styles.statusIndicator}>
        <span className={styles.icon}>{getStatusIcon()}</span>
        <span className={styles.text}>{getStatusText()}</span>
      </div>
      
      {isConnected && hasActiveQueries && (
        <div className={styles.activeQueries}>
          <span className={styles.queryCount}>
            {status?.active_queries_count || 0} Active Queries
          </span>
        </div>
      )}
      
      {lastUpdate && (
        <div className={styles.lastUpdate}>
          Last update: {formatLastUpdate()}
        </div>
      )}
      
      {error && (
        <div className={styles.errorMessage} title={error}>
          {error.length > 50 ? `${error.substring(0, 50)}...` : error}
        </div>
      )}
    </div>
  );
}; 