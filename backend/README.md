# Final Tennis Bot Project

## Overview
The Final Tennis Bot project is designed to fetch, merge, and display tennis match data from multiple APIs. The project consists of a backend implemented with FastAPI and a frontend implemented with React. The backend fetches data from BetsAPI and RapidAPI, merges the data, and serves it to the frontend for display.

## Project Structure
```
FinalTennisBot/
    aggregator/
        sports/
            tennis/
                betsapi_prematch.py
                market_grouper.py
                rapid_tennis_fetcher.py
                tennis_bot.py
                tennis_merger.py
    FinalTennisBot.egg-info/
    setup.py
    tennis_bot_counters.json
    tennis_bot.log

    tennis-frontend/
        public/
        src/
            components/
                MatchList.js
            App.js
            index.js
        package.json
        README.md

    tennis_bot_env/
```

## Backend
The backend is responsible for fetching and merging tennis match data from BetsAPI and RapidAPI. It uses FastAPI to serve the data to the frontend and provides a WebSocket endpoint for real-time updates.

### Key Files
- `betsapi_prematch.py`: Fetches prematch data from BetsAPI.
- `market_grouper.py`: Groups betting markets from RapidAPI data.
- `rapid_tennis_fetcher.py`: Fetches in-play tennis odds from RapidAPI.
- `tennis_bot.py`: Orchestrates the data fetching, merging, and serving processes.
- `tennis_merger.py`: Merges data from BetsAPI and RapidAPI.

### Running the Backend
To start the backend server, run the following command:
```
uvicorn FinalTennisBot.aggregator.sports.tennis.tennis_bot:app --host 0.0.0.0 --port 8000
```

## Frontend
The frontend is a React application that displays the merged tennis match data. It connects to the backend via WebSocket for real-time updates and also provides a REST API endpoint for fetching data.

### Key Files
- `MatchList.js`: Component for displaying the list of tennis matches.
- `App.js`: Main application component.
- `index.js`: Entry point for the React application.

### Running the Frontend
To start the React development server, run the following command:
```
npm start --prefix tennis-frontend
```

## What Has Been Done So Far
1. Implemented the backend to fetch data from BetsAPI and RapidAPI.
2. Merged the data from both APIs using `tennis_merger.py`.
3. Created a FastAPI server to serve the merged data and provide a WebSocket endpoint for real-time updates.
4. Implemented the React frontend to display the merged tennis match data.
5. Ensured the frontend connects to the backend via WebSocket and REST API.
6. Handled WebSocket connection errors and ensured proper data display.

## Next Steps
1. **Testing and Validation**: Thoroughly test the backend and frontend to ensure data is fetched, merged, and displayed correctly.
2. **Error Handling**: Improve error handling in both backend and frontend to handle edge cases and unexpected errors.
3. **Deployment**: Deploy the backend and frontend to a production environment.
4. **Documentation**: Expand the documentation to include detailed API endpoints, data structures, and usage examples.
5. **Performance Optimization**: Optimize the data fetching and merging processes for better performance.
6. **Additional Features**: Add more features such as filtering, sorting, and searching for matches in the frontend.

## Conclusion
The Final Tennis Bot project provides a comprehensive solution for fetching, merging, and displaying tennis match data from multiple APIs. With the next steps outlined, the project can be further improved and deployed to a production environment for real-world use.
