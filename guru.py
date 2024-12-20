import json
import os
import sys
import pandas as pd
import logging
from analyzer import SongAnalyzer
from data import DatabaseManager, LyricsManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

class Guru:

    @staticmethod
    def build_playlist_json(songs: list[dict], playlist_description: str):
        # Add description to JSON of generated playlists
        generated_playlists = {}
        if os.path.exists("generated_playlists.json"):
            with open("generated_playlists.json", "r") as f:
                generated_playlists = json.load(f)
            
            if playlist_description in generated_playlists.keys():
                generated_playlists[playlist_description].append(song for song in songs)
            else:
                generated_playlists[playlist_description] = [songs]
        else:
            generated_playlists = {playlist_description: [songs]}

        with open("generated_playlists.json", "w") as f:
            json.dump(generated_playlists, f)

    
    @staticmethod
    def build_playlist(playlist_description: str, song_limit: int = 20):
        # Get song as DataFrame from database
        all_songs = DatabaseManager.select(db_path, "SELECT * FROM songs", None)
        song_dicts = all_songs.to_dict(orient="records")

        songs_analyzed, songs_to_add = [], []

        # Analyze songs in batches of 5 in case we reach the song limit
        batches = [song_dicts[i:i + 5] for i in range(0, len(song_dicts), 5)]

        for batch in batches:
            if len(songs_to_add) >= song_limit:
                break

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
        
        # Add songs to playlist
        Guru.build_playlist_json(songs_to_add, playlist_description)

        logging.info(f"Generated '{playlist_description}' playlist with {len(songs_to_add)} songs.")
            
if __name__ == "__main__":
    db_path = "song_data.db"
    DatabaseManager.initialize(db_path)

    playlist_description = input("Enter playlist description: ")

    Guru.build_playlist(playlist_description)



    sys.exit(0)

    # Load playlist
    playlist_path = "spotify_playlist.csv"
    playlist = pd.read_csv(playlist_path)
    DatabaseManager.execute_query(
        db_path,
        "INSERT OR IGNORE INTO songs (track_name, artist, album, release_date, duration_ms, spotify_id) VALUES (?, ?, ?, ?, ?, ?)", 
        playlist[["Track Name", "Artist(s)", "Album", "Release Date", "Duration (ms)", "Spotify ID"]].values.tolist()
    )

    # Update lyrics in database
    LyricsManager.update_lyrics_mp(db_path, num_processes=1)

    # Fetch and update audio features from Spotify to the database
    DatabaseManager.update_audio_features(db_path)

    # Analyze lyrics
    # LyricsManager.update_lyrics_analysis(db_path)

    # Analyze lyrics with AI
    # LyricsManager.update_lyrics_analysis(db_path)

    logging.info("All updates completed.")
