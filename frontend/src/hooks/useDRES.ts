import { useState, useCallback } from 'react';
import axios from 'axios';

// DRES API client configuration
const dresApi = axios.create({
  baseURL: 'http://localhost:5000/api/dres',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Types for DRES integration
export interface DRESSubmission {
  query_id: string;
  video_id: string;
  timestamp: number;
  confidence?: number;
  segment_start?: number;
  segment_end?: number;
}

export interface DRESStatus {
  connected: boolean;
  timestamp: string;
  competition?: any;
  active_queries_count?: number;
}

export interface DRESQuery {
  id: string;
  title?: string;
  description?: string;
  type?: string;
  status?: string;
}

export interface DRESSubmissionResult {
  success: boolean;
  message: string;
  submission?: DRESSubmission;
}

export interface DRESBatchResult {
  success: boolean;
  message: string;
  results: Record<string, boolean>;
  summary: {
    total: number;
    successful: number;
    failed: number;
  };
}

export const useDRES = () => {
  const [status, setStatus] = useState<DRESStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Get DRES connection status
  const getStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await dresApi.get('/status');
      setStatus(response.data);
      return response.data;
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to get DRES status';
      setError(errorMessage);
      console.error('DRES status error:', err);
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  // Submit a single result to DRES
  const submitResult = useCallback(async (submission: DRESSubmission): Promise<DRESSubmissionResult> => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await dresApi.post('/submit', submission);
      return response.data;
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to submit result';
      setError(errorMessage);
      console.error('DRES submission error:', err);
      return {
        success: false,
        message: errorMessage
      };
    } finally {
      setLoading(false);
    }
  }, []);

  // Submit multiple results to DRES
  const submitBatch = useCallback(async (submissions: DRESSubmission[]): Promise<DRESBatchResult> => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await dresApi.post('/submit-batch', submissions);
      return response.data;
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to submit batch';
      setError(errorMessage);
      console.error('DRES batch submission error:', err);
      return {
        success: false,
        message: errorMessage,
        results: {},
        summary: {
          total: submissions.length,
          successful: 0,
          failed: submissions.length
        }
      };
    } finally {
      setLoading(false);
    }
  }, []);

  // Get active queries from DRES
  const getActiveQueries = useCallback(async (): Promise<DRESQuery[]> => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await dresApi.get('/queries');
      return response.data.queries || [];
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to get active queries';
      setError(errorMessage);
      console.error('DRES queries error:', err);
      return [];
    } finally {
      setLoading(false);
    }
  }, []);

  // Get query information
  const getQueryInfo = useCallback(async (queryId: string): Promise<any> => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await dresApi.get(`/query/${queryId}`);
      return response.data;
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to get query info';
      setError(errorMessage);
      console.error('DRES query info error:', err);
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  // Get submission history
  const getSubmissionHistory = useCallback(async (queryId?: string): Promise<any[]> => {
    setLoading(true);
    setError(null);
    
    try {
      const url = queryId ? `/history?query_id=${queryId}` : '/history';
      const response = await dresApi.get(url);
      return response.data.history || [];
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to get submission history';
      setError(errorMessage);
      console.error('DRES history error:', err);
      return [];
    } finally {
      setLoading(false);
    }
  }, []);

  // Test DRES connection
  const testConnection = useCallback(async (): Promise<boolean> => {
    setLoading(true);
    setError(null);
    
    try {
      const status = await getStatus();
      return status?.connected || false;
    } catch (err: any) {
      const errorMessage = err.response?.data?.message || err.message || 'Failed to test connection';
      setError(errorMessage);
      console.error('DRES connection test error:', err);
      return false;
    } finally {
      setLoading(false);
    }
  }, [getStatus]);

  // Clear error state
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  return {
    // State
    status,
    loading,
    error,
    
    // Actions
    getStatus,
    submitResult,
    submitBatch,
    getActiveQueries,
    getQueryInfo,
    getSubmissionHistory,
    testConnection,
    clearError,
    
    // Computed values
    isConnected: status?.connected || false,
    hasActiveQueries: (status?.active_queries_count || 0) > 0
  };
};

// Convenience function for submitting from video cards
export const submitVideoToDRES = async (
  queryId: string, 
  videoId: string, 
  timestamp: number, 
  confidence: number = 1.0
): Promise<DRESSubmissionResult> => {
  try {
    const response = await dresApi.post('/submit', {
      query_id: queryId,
      video_id: videoId,
      timestamp: timestamp,
      confidence: confidence
    });
    return response.data;
  } catch (err: any) {
    const errorMessage = err.response?.data?.message || err.message || 'Failed to submit to DRES';
    console.error('DRES submission error:', err);
    return {
      success: false,
      message: errorMessage
    };
  }
}; 