import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { AppBar, Toolbar, Typography, Container, CircularProgress, Button, Box } from '@mui/material';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import MatchList from './components/MatchList';
import RawDataViewer from './components/RawDataViewer';
import './App.css';

function App() {
  const [matches, setMatches] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Initial data load from REST API
    const fetchInitialData = async () => {
      try {
        console.log('Fetching initial data...');
        const response = await axios.get('http://localhost:8000/api/tennis');
        console.log('Received data:', response.data);
        if (response.data?.matches) {
          setMatches(response.data.matches);
        } else {
          console.error('No matches data in response:', response.data);
          setError('No matches data available');
        }
        setLoading(false);
      } catch (err) {
        console.error('Error fetching data:', err);
        setError(`Failed to fetch initial data: ${err.message}`);
        setLoading(false);
      }
    };

    // WebSocket connection for real-time updates
    const connectWebSocket = () => {
      console.log('Connecting to WebSocket...');
      const ws = new WebSocket('ws://localhost:8000/ws');
      let reconnectTimer = null;
      
      ws.onopen = () => {
        console.log('WebSocket connected successfully');
        setError(null); // Clear any previous errors on successful connection
      };

      ws.onmessage = (event) => {
        try {
          console.log('WebSocket message received');
          const data = JSON.parse(event.data);
          if (data?.matches) {
            console.log(`Received ${data.matches.length} matches`);
            setMatches(data.matches);
          } else {
            console.error('Invalid data format received:', data);
          }
        } catch (err) {
          console.error('Error processing WebSocket message:', err);
        }
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        setError('WebSocket connection error - retrying...');
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected');
        // Try to reconnect after 5 seconds
        reconnectTimer = setTimeout(connectWebSocket, 5000);
      };

      return () => {
        if (reconnectTimer) {
          clearTimeout(reconnectTimer);
        }
        ws.close();
      };
    };

    fetchInitialData();
    const cleanup = connectWebSocket();

    return cleanup;
  }, []);

  return (
    <Router>
      <div className="App">
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6" sx={{ flexGrow: 1 }}>
              Tennis Bot Dashboard
            </Typography>
            <Box>
              <Button color="inherit" component={Link} to="/">
                Matches
              </Button>
              <Button color="inherit" component={Link} to="/raw-data">
                Raw Data
              </Button>
            </Box>
          </Toolbar>
        </AppBar>
        
        <Routes>
          <Route 
            path="/" 
            element={
              <Container sx={{ mt: 3 }}>
                {loading ? (
                  <CircularProgress />
                ) : error ? (
                  <Typography color="error">{error}</Typography>
                ) : (
                  <MatchList matches={matches} />
                )}
              </Container>
            } 
          />
          <Route path="/raw-data" element={<RawDataViewer />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
