import json
import os
import sys
import time
import logging
from analyzer import SongAnalyzer
from data import DatabaseManager, LyricsManager

generated_playlists_folder = "generated_playlists"
generated_playlists_path = f"{generated_playlists_folder}/generated_playlists.json"
if not os.path.exists(generated_playlists_folder):
    os.makedirs(generated_playlists_folder)

db_path = "song_data.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

class Guru:

    @staticmethod
    def build_playlist_json(songs: list[dict], playlist_title: str, playlist_description: str):
        # Add description to JSON of generated playlists
        generated_playlists = {}
        if os.path.exists(generated_playlists_path):
            with open(generated_playlists_path, "r") as f:
                generated_playlists = json.load(f)
            
            if playlist_title in generated_playlists.keys():
                generated_playlists[playlist_title]['songs'].append(song for song in songs if song not in generated_playlists[playlist_title]['songs'])
            else:
                generated_playlists[playlist_title]['songs'] = [songs]
        else:
            generated_playlists = {playlist_title: {'description': playlist_description, 'songs': [songs]}}

        with open(generated_playlists_path, "w") as f:
            json.dump(generated_playlists, f)

    
    @staticmethod
    def build_playlist(playlist_title: str,playlist_description: str, song_limit: int = 10):
        # Get song as DataFrame from database
        all_songs = DatabaseManager.select(db_path, "SELECT * FROM songs", None)
        song_dicts = all_songs.to_dict(orient="records")

        songs_analyzed, songs_to_add = [], []

        # Analyze songs in batches of 5 to limit token coint and in case we reach the song limit
        batches = [song_dicts[i:i + 5] for i in range(0, len(song_dicts), 5)]

        for batch in batches:
            if len(songs_to_add) >= song_limit:
                break
            
            logging.info(f"Analyzing {len(batch)} songs...")
            results: dict = SongAnalyzer.analyze_song_fits_groq(batch, playlist_description)

            for song in batch:
                songs_analyzed.append((song["track_name"], song["artist"]))

            for index, data in results.items():
                if "YUP" in data['result']:
                    song_data = {
                        "track_name": batch[index]["track_name"], 
                        "artist": batch[index]["artist"], 
                        "explanation": data['explanation'], 
                        "spotify_id": batch[index]["spotify_id"]
                    }
                    songs_to_add.append(song_data)
            
            logging.info(f"Found {len(songs_to_add)}/{song_limit} total songs that fit playlist description.")
        
        # Add songs to playlist
        Guru.build_playlist_json(songs_to_add, playlist_title, playlist_description)

        logging.info(f"Generated '{playlist_title} - {playlist_description}' playlist with {len(songs_to_add)} songs.")
            
if __name__ == "__main__":
    # DatabaseManager.initialize(db_path)

    # # Load each CSV from playlist_data folder
    # playlist_paths = [path for path in os.listdir("playlist_data") if path.endswith(".csv")]
    # for playlist_path in playlist_paths:
    #     full_path = os.path.join("playlist_data", playlist_path)
    #     DatabaseManager.insert_playlist(db_path, full_path)

    # # Get lyrics from API and update in database
    # LyricsManager.update_lyrics_mp(db_path, num_processes=1)

    # # Fetch and update audio features from Spotify to the database
    # DatabaseManager.update_audio_features(db_path)

    # Use LLM to analyze song data and generate playlist based on description and title
    # playlist_title = input("Enter playlist title: ")
    # playlist_description = input("Enter playlist description: ")

    playlist_title = "Optimistic"
    playlist_description = "An optimistic playlist that focuses on themes of self-efficacy, optimistic futures, uplifting, inspirational"

    Guru.build_playlist(playlist_title, playlist_description)

    logging.info("All updates completed.")
