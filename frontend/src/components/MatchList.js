import React, { useEffect } from 'react';
import { 
  Card,
  CardContent,
  Typography,
  Grid,
  Container,
  Divider,
  Box,
  Chip
} from '@mui/material';

const MatchList = ({ matches }) => {
  // Log the match data to see what's available
  useEffect(() => {
    if (matches && matches.length > 0) {
      console.log('Sample match data:', matches[0]);
    }
  }, [matches]);

  if (!matches || matches.length === 0) {
    return (
      <Container>
        <Typography variant="body1">No tennis matches currently available</Typography>
      </Container>
    );
  }

  return (
    <Container>
      <Grid container spacing={2}>
        {matches.map((match, index) => {
          const rapidData = match.rapid_data?.raw_event_data;
          const betsData = match.betsapi_data;
          
          // Log individual match data
          console.log(`Match ${index} data:`, {
            rapidData: match.rapid_data,
            betsData: match.betsapi_data
          });
          
          return (
            <Grid item xs={12} key={index}>
              <Card>
                <CardContent>
                  {/* Header with match info and status */}
                  <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                    <Typography variant="h6" component="div">
                      {rapidData ? 
                        `${rapidData.team1} vs ${rapidData.team2}` :
                        betsData?.inplay_event ? 
                          `${betsData.inplay_event.home?.name || ''} vs ${betsData.inplay_event.away?.name || ''}` :
                          'Unknown Match'
                      }
                    </Typography>
                    {(betsData?.inplay_event?.status || rapidData?.state) && (
                      <Chip 
                        color={betsData?.inplay_event?.status === "in-play" || rapidData?.state === "in_play" ? "success" : "default"}
                        size="small"
                        label={betsData?.inplay_event?.status || rapidData?.state || "Unknown"} 
                      />
                    )}
                  </Box>

                  {/* League/Tournament (from RapidAPI or BetsAPI) */}
                  {(rapidData?.competitionName || betsData?.league?.name) && (
                    <Typography variant="subtitle2" color="textSecondary" gutterBottom>
                      Tournament: {rapidData?.competitionName || betsData?.league?.name || "Unknown Tournament"}
                    </Typography>
                  )}
                  
                  {/* Match Time */}
                  {(rapidData?.startTime || betsData?.time) && (
                    <Typography variant="body2" color="textSecondary" gutterBottom>
                      Time: {
                        rapidData?.startTime ? 
                          new Date(rapidData.startTime).toLocaleString() : 
                          betsData?.time ? 
                            new Date(betsData.time * 1000).toLocaleString() : 
                            "Unknown"
                      }
                    </Typography>
                  )}
                  
                  {/* Score Information - Check specific paths in the actual data structure */}
                  {(rapidData?.score || betsData?.scores) && (
                    <Box my={1}>
                      <Typography variant="subtitle2">Current Score:</Typography>
                      {rapidData?.score && (
                        <Typography variant="body2">
                          Score from Rapid API: {JSON.stringify(rapidData.score)}
                        </Typography>
                      )}
                      {betsData?.scores && (
                        <Typography variant="body2">
                          Score from BetsAPI: {JSON.stringify(betsData.scores)}
                        </Typography>
                      )}
                    </Box>
                  )}
                  
                  {/* Additional BetsAPI specific data */}
                  {betsData && (
                    <Box my={1}>
                      <Typography variant="subtitle2" gutterBottom>BetsAPI Details:</Typography>
                      {betsData.sport_id && (
                        <Typography variant="body2">Sport ID: {betsData.sport_id}</Typography>
                      )}
                      {betsData.time_status !== undefined && (
                        <Typography variant="body2">Time Status: {betsData.time_status}</Typography>
                      )}
                      {betsData.league && betsData.league.cc && (
                        <Typography variant="body2">Country: {betsData.league.cc}</Typography>
                      )}
                    </Box>
                  )}
                  
                  {/* RapidAPI specific data */}
                  {rapidData && (
                    <Box my={1}>
                      <Typography variant="subtitle2" gutterBottom>RapidAPI Details:</Typography>
                      {rapidData.inPlay !== undefined && (
                        <Typography variant="body2">In Play: {rapidData.inPlay ? "Yes" : "No"}</Typography>
                      )}
                      {rapidData.sportId && (
                        <Typography variant="body2">Sport ID: {rapidData.sportId}</Typography>
                      )}
                      {rapidData.marketCount && (
                        <Typography variant="body2">Markets: {rapidData.marketCount}</Typography>
                      )}
                    </Box>
                  )}
                  
                  <Divider sx={{ my: 1 }} />
                  
                  {/* IDs and technical info */}
                  <Grid container spacing={1}>
                    {/* Display Event ID if available */}
                    {rapidData?.eventId && (
                      <Grid item xs={12} sm={6}>
                        <Typography variant="body2" color="textSecondary">
                          Event ID: {rapidData.eventId}
                        </Typography>
                      </Grid>
                    )}
                    
                    {/* Display Bet365 ID if available */}
                    {(betsData?.bet365_id || rapidData?.marketFI) && (
                      <Grid item xs={12} sm={6}>
                        <Typography variant="body2" color="textSecondary">
                          Bet365 ID: {betsData?.bet365_id || rapidData?.marketFI}
                        </Typography>
                      </Grid>
                    )}
                  </Grid>
                  
                  {/* Match Status and Data Source */}
                  <Box mt={1}>
                    <Typography variant="body2" color="textSecondary">
                      Data Source: {
                        match.rapid_data && match.betsapi_data ? 'Both APIs' :
                        match.rapid_data ? 'RapidAPI Only' :
                        match.betsapi_data ? 'BetsAPI Only' : 'Unknown'
                      }
                    </Typography>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>
    </Container>
  );
};

export default MatchList;