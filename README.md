# Tennis Bot Dashboard

A real-time tennis match tracking application with data aggregation from multiple APIs.

## Project Structure

This repository contains:

- **Backend** (`/backend`): A Python FastAPI application that:
  - Fetches tennis match data from BetsAPI and RapidAPI
  - Merges the data from both sources
  - Provides both REST API and WebSocket endpoints

- **Frontend** (`/frontend`): A React application that:
  - Displays tennis matches in a user-friendly interface
  - Shows detailed match information including scores and statistics
  - Provides a raw data view for debugging and development
  - Updates in real-time using WebSocket connection

## Getting Started

### Backend Setup

```bash
cd backend
python -m pip install -e .
python -m aggregator.sports.tennis.tennis_bot
```

### Frontend Setup

```bash
cd frontend
npm install
npm start
```

Access the frontend at http://localhost:3000

The raw tennis data can be viewed at http://localhost:3000/raw-data