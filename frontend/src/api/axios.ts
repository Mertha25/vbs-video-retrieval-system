// This file sets up an Axios instance for making HTTP requests to the Flask backend.
// In fake-data mode, it will not be used, but it's here for completeness.

// Stub axios instance (not used in fake-data mode)
import axios from 'axios';

const API = axios.create({
  baseURL: 'http://localhost:5000/api', // Flask backend runs on port 5000
});

export default API;
