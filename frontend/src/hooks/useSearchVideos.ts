// frontend/src/hooks/useSearchVideos.ts
import { useState } from 'react';
import API from '../api/axios';
import { FilterCriteria } from '../components/FilterPanel/FilterPanel';

// Utility: Convert hex color (#rrggbb) to RGB array [r, g, b]
const hexToRgbArray = (hex: string): number[] => [
  parseInt(hex.substring(1, 3), 16),
  parseInt(hex.substring(3, 5), 16),
  parseInt(hex.substring(5, 7), 16),
];

// Utility: Convert time string "HH:MM:SS" to seconds
const timeStrToSeconds = (t: string): number => {
  const [h, m, s] = t.split(':').map(Number);
  return h * 3600 + m * 60 + s;
};

// Hook to handle video search using FilterCriteria from FilterPanel
export function useSearchVideos() {
  const [results, setResults] = useState([]);        // search results
  const [loading, setLoading] = useState(false);     // loading spinner state
  const [error, setError] = useState<string | null>(null); // error state
  const [extractedKeywords, setExtractedKeywords] = useState<string[]>([]); // extracted keywords
  const [originalQuery, setOriginalQuery] = useState<string>(''); // original user query

  /**
   * Main search function that routes to different backend endpoints
   * based on which fields in `FilterCriteria` are active
   */
  const searchWithCriteria = async (criteria: FilterCriteria) => {
    setLoading(true);
    setError(null);
    setResults([]);
    setExtractedKeywords([]);
    setOriginalQuery('');

    try {
      const payload: any = {};
      if (criteria.text) payload.text = criteria.text;
      if (criteria.color) payload.color = hexToRgbArray(criteria.color);
      if (criteria.objects) payload.objects = criteria.objects;
      if (criteria.words) payload.words = criteria.words;
      if (criteria.interval) {
        payload.start_time = timeStrToSeconds(criteria.interval.from);
        payload.end_time = timeStrToSeconds(criteria.interval.to);
      }
      if (criteria.embedding) payload.embedding = criteria.embedding;
      const res = await API.post('/search/multimodal', payload);
      setResults(res.data.results || []);
      
      // Handle extracted keywords if present in response
      if (res.data.extracted_keywords) {
        setExtractedKeywords(res.data.extracted_keywords);
      }
      if (res.data.original_text) {
        setOriginalQuery(res.data.original_text);
      }
    } catch (err: any) {
      setError(err.message || "Search failed");
    } finally {
      setLoading(false);
    }
  };

  return {
    results,
    loading,
    error,
    extractedKeywords,
    originalQuery,
    searchWithCriteria
  };
}
