import React, { useState, useEffect } from 'react';
import { useDRES, DRESQuery } from '../../hooks/useDRES';
import styles from './VBSCompetitionMode.module.css';

export interface VBSCompetitionModeProps {
  onQuerySelect?: (queryId: string) => void;
  currentQueryId?: string;
  enableDRES?: boolean;
}

export const VBSCompetitionMode: React.FC<VBSCompetitionModeProps> = ({
  onQuerySelect,
  currentQueryId,
  enableDRES = false
}) => {
  const { 
    status, 
    loading, 
    error, 
    getStatus, 
    getActiveQueries, 
    getQueryInfo,
    isConnected,
    hasActiveQueries 
  } = useDRES();

  const [activeQueries, setActiveQueries] = useState<DRESQuery[]>([]);
  const [selectedQuery, setSelectedQuery] = useState<DRESQuery | null>(null);
  const [showQueryDetails, setShowQueryDetails] = useState(false);

  // Load active queries when connected
  useEffect(() => {
    if (isConnected && hasActiveQueries) {
      loadActiveQueries();
    }
  }, [isConnected, hasActiveQueries]);

  // Load query details when currentQueryId changes
  useEffect(() => {
    if (currentQueryId && isConnected) {
      loadQueryDetails(currentQueryId);
    }
  }, [currentQueryId, isConnected]);

  const loadActiveQueries = async () => {
    const queries = await getActiveQueries();
    setActiveQueries(queries);
  };

  const loadQueryDetails = async (queryId: string) => {
    const queryInfo = await getQueryInfo(queryId);
    if (queryInfo) {
      setSelectedQuery({
        id: queryId,
        title: queryInfo.title || queryInfo.description,
        description: queryInfo.description,
        type: queryInfo.type,
        status: queryInfo.status
      });
    }
  };

  const handleQuerySelect = (query: DRESQuery) => {
    setSelectedQuery(query);
    onQuerySelect?.(query.id);
  };

  const refreshStatus = () => {
    getStatus();
    if (isConnected) {
      loadActiveQueries();
    }
  };

  if (!enableDRES) {
    return null;
  }

  return (
    <div className={styles.vbsCompetitionMode}>
      <div className={styles.header}>
        <h3>🎯 VBS Competition Mode</h3>
        <button 
          className={styles.refreshBtn} 
          onClick={refreshStatus}
          disabled={loading}
        >
          {loading ? '🔄' : '🔄'}
        </button>
      </div>

      {/* DRES Connection Status */}
      <div className={styles.statusSection}>
        <div className={`${styles.statusIndicator} ${isConnected ? styles.connected : styles.disconnected}`}>
          <span className={styles.statusIcon}>
            {isConnected ? '✅' : '❌'}
          </span>
          <span className={styles.statusText}>
            DRES: {isConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
        
        {error && (
          <div className={styles.errorMessage}>
            Error: {error}
          </div>
        )}
      </div>

      {/* Active Queries */}
      {isConnected && (
        <div className={styles.queriesSection}>
          <h4>Active Queries ({activeQueries.length})</h4>
          
          {activeQueries.length === 0 ? (
            <p className={styles.noQueries}>No active queries available</p>
          ) : (
            <div className={styles.queriesList}>
              {activeQueries.map((query) => (
                <div 
                  key={query.id}
                  className={`${styles.queryItem} ${currentQueryId === query.id ? styles.selected : ''}`}
                  onClick={() => handleQuerySelect(query)}
                >
                  <div className={styles.queryId}>Query {query.id}</div>
                  {query.title && (
                    <div className={styles.queryTitle}>{query.title}</div>
                  )}
                  {query.type && (
                    <div className={styles.queryType}>{query.type}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Selected Query Details */}
      {selectedQuery && (
        <div className={styles.selectedQuerySection}>
          <h4>Selected Query</h4>
          <div className={styles.queryDetails}>
            <div className={styles.queryDetail}>
              <strong>ID:</strong> {selectedQuery.id}
            </div>
            {selectedQuery.title && (
              <div className={styles.queryDetail}>
                <strong>Title:</strong> {selectedQuery.title}
              </div>
            )}
            {selectedQuery.description && (
              <div className={styles.queryDetail}>
                <strong>Description:</strong> {selectedQuery.description}
              </div>
            )}
            {selectedQuery.type && (
              <div className={styles.queryDetail}>
                <strong>Type:</strong> {selectedQuery.type}
              </div>
            )}
            {selectedQuery.status && (
              <div className={styles.queryDetail}>
                <strong>Status:</strong> {selectedQuery.status}
              </div>
            )}
          </div>
          
          <button 
            className={styles.detailsBtn}
            onClick={() => setShowQueryDetails(!showQueryDetails)}
          >
            {showQueryDetails ? 'Hide Details' : 'Show Details'}
          </button>
        </div>
      )}

      {/* Competition Instructions */}
      <div className={styles.instructions}>
        <h4>How to Use</h4>
        <ol>
          <li>Ensure DRES is connected (green status above)</li>
          <li>Select an active query from the list</li>
          <li>Search for videos using the search interface</li>
          <li>Click "Submit to DRES" on any video result</li>
          <li>Adjust timestamp if needed and confirm submission</li>
        </ol>
      </div>
    </div>
  );
}; 