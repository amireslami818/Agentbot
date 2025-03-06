import React, { useState, useEffect } from 'react';
import { 
  Box, 
  Typography, 
  Paper,
  Button,
  CircularProgress,
  Container
} from '@mui/material';

const RawDataViewer = () => {
  const [rawData, setRawData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const fetchRawData = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:8000/api/tennis/raw');
      const data = await response.json();
      console.log('Raw data fetched successfully:', data);
      setRawData(data);
      setLastUpdated(new Date().toLocaleTimeString());
      setError(null);
    } catch (err) {
      console.error('Error fetching raw data:', err);
      setError(`Failed to fetch data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRawData();
    
    // Set up automatic refresh every minute
    const interval = setInterval(fetchRawData, 60000);
    
    // Clean up interval on component unmount
    return () => clearInterval(interval);
  }, []);

  return (
    <Container>
      <Box sx={{ my: 4 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h5" component="h2">Raw Tennis Data</Typography>
          <Box>
            <Button 
              variant="contained" 
              onClick={fetchRawData} 
              disabled={loading}
              sx={{ mr: 2 }}
            >
              Refresh Data
            </Button>
            {lastUpdated && (
              <Typography variant="body2" component="span">
                Last updated: {lastUpdated}
              </Typography>
            )}
          </Box>
        </Box>
        
        {loading ? (
          <Box display="flex" justifyContent="center" my={4}>
            <CircularProgress />
          </Box>
        ) : error ? (
          <Typography color="error">{error}</Typography>
        ) : rawData ? (
          <Paper 
            elevation={3} 
            sx={{ 
              p: 2, 
              maxHeight: '80vh', 
              overflow: 'auto',
              fontFamily: 'monospace'
            }}
          >
            <pre style={{ whiteSpace: 'pre-wrap' }}>
              {JSON.stringify(rawData, null, 2)}
            </pre>
          </Paper>
        ) : (
          <Typography>No data available</Typography>
        )}
      </Box>
    </Container>
  );
};

export default RawDataViewer;