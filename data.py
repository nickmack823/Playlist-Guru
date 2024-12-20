import sqlite3
import pandas as pd
import requests
import logging
from multiprocessing import Queue, Process
from transformers import pipeline

from analyzer import SongAnalyzer
from spotify_api import SpotifyAPI

class DatabaseManager:
    @staticmethod
    def initialize(db_path: str):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_name TEXT NOT NULL,
                artist TEXT NOT NULL,
                album TEXT,
                release_date TEXT,
                duration_ms INTEGER,
                spotify_id TEXT,
                lyrics TEXT,
                sentiment_score REAL,
                dominant_emotion TEXT,
                acousticness REAL,
                danceability REAL,
                energy REAL,
                instrumentalness REAL,
                key TEXT,
                liveness REAL,
                loudness REAL,
                modality INTEGER,
                speechiness REAL,
                tempo REAL,
                time_signature INTEGER,
                valence REAL,
                UNIQUE(track_name, artist)
            )
        ''')
        conn.commit()
        conn.close()

    @staticmethod
    def execute_query(db_path: str, query: str, params=None) -> list:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if isinstance(params, list) and isinstance(params[0], (list, tuple)):
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params or [])
        result = cursor.fetchall()
        conn.commit()
        conn.close()
        
        return result
    
    @staticmethod
    def select(db_path: str, query: str, params=None) -> pd.DataFrame:
        """
        Executes a SELECT query and returns the result as a Pandas DataFrame.

        :param db_path: Path to the SQLite database file.
        :param query: SQL SELECT query to execute.
        :param params: Optional parameters to pass with the query.
        :return: DataFrame containing the query results.
        """
        # Establish a connection to the SQLite database
        conn = sqlite3.connect(db_path)
        try:
            # Execute the query and fetch the result into a DataFrame
            df = pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            # Log or handle the exception as needed
            logging.error(f"Error executing query: {e}")
            df = pd.DataFrame()  # Return an empty DataFrame in case of error
        finally:
            # Ensure the connection is closed after the operation
            conn.close()
        return df
    
    @staticmethod
    def update(db_path: str, query: str, params):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        conn.close()

    # Retrieve and update audio features for database songs
    @staticmethod
    def update_audio_features(db_path: str):
        logging.info("Updating audio features...")
        songs = pd.DataFrame(DatabaseManager.execute_query(
            db_path, 
            "SELECT spotify_id FROM songs WHERE acousticness IS NULL"), 
            columns=["spotify_id"]
        )
        
        if songs.empty:
            logging.info("No songs to update audio features for.")
            return

        song_ids = songs["spotify_id"].tolist()
        features = SpotifyManager.fetch_audio_features(song_ids)

        for _, row in features.iterrows():

            # Convert key from int to str note representation
            if row['key'] is not None:
                row['key'] = SongAnalyzer.key_mapper(row['key'])

            DatabaseManager.update(
                db_path,
                '''UPDATE songs SET acousticness = ?, danceability = ?, energy = ?, instrumentalness = ?, key = ?,
                   liveness = ?, loudness = ?, modality = ?, speechiness = ?, tempo = ?, time_signature = ?, valence = ?
                   WHERE spotify_id = ?''',
                (
                    row['acousticness'], row['danceability'], row['energy'], row['instrumentalness'], row['key'],
                    row['liveness'], row['loudness'], row['mode'], row['speechiness'], row['tempo'],
                    row['time_signature'], row['valence'], row['id']
                )
            )
        
        logging.info("Audio features updated.")
    
class LyricsManager:
    @staticmethod
    def get_lyrics(track_name: str, artist: str) -> str:
        try:
            response = requests.get(f"https://lrclib.net/api/get", params={"artist_name": artist, "track_name": track_name})
            if response.status_code == 200:
                data = response.json()
                return data.get("plainLyrics") or "Lyrics not found."
            return "Lyrics not found."
        except Exception as e:
            logging.error(f"Error fetching lyrics for {track_name} by {artist}: {e}")
            return None
        
    @staticmethod
    def _lyrics_data_worker(queue: Queue, db_path: str, progress_queue: Queue):
        while not queue.empty():
            song_id, track_name, artist = queue.get()
            lyrics = LyricsManager.get_lyrics(track_name, artist)
            if lyrics:
                DatabaseManager.update(db_path, "UPDATE songs SET lyrics = ? WHERE id = ?", (lyrics, song_id))
            progress_queue.put(1)

    
    @staticmethod
    def update_lyrics_mp(db_path: str, num_processes: int = 4):
        rows = DatabaseManager.execute_query(db_path, "SELECT id, track_name, artist FROM songs WHERE lyrics IS NULL")
        
        if not rows:
            logging.info("No songs to update lyrics for.")
            return
        
        queue, progress_queue = Queue(), Queue()

        logging.info(f"Updating {len(rows)} lyrics...")

        for row in rows:
            queue.put(row)

        processes = [Process(target=LyricsManager._lyrics_data_worker, args=(queue, db_path, progress_queue)) for _ in range(num_processes)]
        for process in processes:
            process.start()

        processed = 0
        while processed < len(rows):
            processed += progress_queue.get()
            logging.info(f"Progress: {processed}/{len(rows)} lyrics updated.")

        for process in processes:
            process.join()
    
        logging.info("Lyrics updated.")

    @staticmethod
    def update_lyrics_analysis(db_path: str):
        rows = DatabaseManager.execute_query(db_path, "SELECT id, lyrics FROM songs WHERE sentiment_score IS NULL OR dominant_emotion IS NULL")
        
        logging.info(f"Analyzing {len(rows)} lyrics...")

        sentiment_pipeline = pipeline("sentiment-analysis")
        emotion_pipeline = pipeline("text-classification", model="SamLowe/roberta-base-go_emotions")

        for row in rows:
            song_id, lyrics = row
            sentiment_score, dominant_emotion = SongAnalyzer.analyze_lyrics_sentiment(lyrics, sentiment_pipeline, emotion_pipeline)
            DatabaseManager.update(db_path, "UPDATE songs SET sentiment_score = ?, dominant_emotion = ? WHERE id = ?", (sentiment_score, dominant_emotion, song_id))
            
            logging.info(f"{rows.index(row) + 1}/{len(rows)} lyrics analyzed and updated in database.")

        logging.info("Lyrical analysis complete.")

class SpotifyManager:
    @staticmethod
    def fetch_audio_features(song_ids: list[str]) -> pd.DataFrame:
        spotify = SpotifyAPI.authorize(client_id='5c098bcc800e45d49e476265bc9b6934', scope='playlist-read-private playlist-read-collaborative user-library-read user-read-playback-state user-read-recently-played')
        all_features = []

        for i in range(0, len(song_ids), 100):
            chunk = song_ids[i:i + 100]

            # Convert chunk of ids to comma-separated string if item is a str
            comma_separated_ids = ''
            for id in chunk:
                if isinstance(id, str):
                    comma_separated_ids += id + ','

            data_chunk = spotify.get(f'audio-features?ids={comma_separated_ids}', {'limit': 100})
            all_features.extend(data_chunk['audio_features'])

        return pd.DataFrame(all_features)
