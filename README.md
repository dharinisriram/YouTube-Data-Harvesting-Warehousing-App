# YouTube Data Harvesting and Warehousing

This project involves collecting data from YouTube channels, storing it in MongoDB and PostgreSQL, and creating a user-friendly web interface using Streamlit for data analysis.

## Overview

- The script interacts with the YouTube Data API to gather information about channels, playlists, videos, and comments.
- Collected data is stored in MongoDB for flexibility and ease of retrieval.
- Data is migrated from MongoDB to PostgreSQL for structured storage.
- A Streamlit web application is created, allowing users to input YouTube channel IDs, collect data, migrate to SQL, and view tables and analysis results.
- SQL queries are executed to answer specific questions about the collected data, and results are displayed in the Streamlit web application.


## Software and Libraries Used

### Streamlit

Streamlit is a Python library used for creating web applications with minimal effort. It allows developers to turn data scripts into shareable web apps quickly.

### Google API Client Library

This library is used to interact with Google APIs, specifically the YouTube Data API in this case. It facilitates the retrieval of channel, video, and comment information from YouTube.

### MongoDB

MongoDB is a NoSQL database used for storing and managing the collected YouTube data. The pymongo library is utilized to interact with MongoDB.

### PostgreSQL

PostgreSQL is a powerful, open-source relational database system. The psycopg2 library is used to connect to and interact with PostgreSQL.

### Pandas

Pandas is a popular data manipulation library in Python. It is used for handling data in tabular form, making it easier to work with datasets.

## ETA (Extract, Transform, Analyze) Process

### Extract

1. **YouTube Data API:**
   - Extract data from YouTube channels, playlists, videos, and comments using the YouTube Data API.

### Transform

2. **Data Transformation:**
   - Transform raw data into a structured format for analysis.
   - Convert and clean data for better usability.

3. **Migrating to MongoDB:**
   - Transform data for MongoDB storage.

4. **Migrating to PostgreSQL:**
   - Transform data for PostgreSQL storage.
   - Execute SQL queries to analyze and transform data.

### Analyze

5. **Streamlit Web Interface:**
   - Create a user-friendly interface using Streamlit to interact with the collected data.
   - Implement features for inputting channel IDs, collecting data, migrating to SQL, and viewing tables and analysis results.

6. **SQL Queries:**
   - Execute SQL queries to answer specific questions about the collected data.
   - Display results in the Streamlit web application.


## Overall Process Flow

### User Input:

Users input YouTube channel IDs into the Streamlit web interface.

### Data Collection:

The script collects data from YouTube using the YouTube Data API.
Information about channels, playlists, videos, and comments is obtained.

### Data Storage in MongoDB:

Collected data is stored in MongoDB for flexibility and ease of retrieval.

### Data Migration to SQL:

Data is fetched from MongoDB and migrated to PostgreSQL for structured storage.

### Streamlit Interface:

Users interact with the Streamlit web application to view tables and execute SQL queries.

### SQL Queries and Data Analysis:

SQL queries are formulated to analyze the data, answering specific questions.
Results are displayed within the Streamlit web application.

## Key Points

- The process integrates data collection from YouTube, storage in both MongoDB and PostgreSQL, and presents the data through a user-friendly web interface using Streamlit.
- MongoDB is used for its flexibility with unstructured data, while PostgreSQL provides a structured relational database for analytical queries.
- Streamlit simplifies the creation of a web application, allowing users to interact with the collected and analyzed YouTube data effortlessly.
