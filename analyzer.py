import logging
import re
import string
import sys
import time
import requests

groq_api_key = "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"

groq_completion_url = 'https://api.groq.com/openai/v1/chat/completions'

groq_models = ['llama-3.3-70b-versatile', 'llama-3.1-70b-versatile']

basic_prompt_template = """
You are a music recommendation assistant.
Analyze the following song data to determine its suitability for the described playlist.

**Playlist Description:**
${playlist_description}

**Task:**
Provide a list item for each song of the format "[Song Title] [Artist] [YUP/NOPE]" with the word "YUP" in your response if the song fits and "NOPE" if it does not.
After each list item, provide a one-sentence explanation of why the song fits or not the playlist followed by an empty line.
Be very careful to respond in the exact format that I have provided.
"""

song_data_template = """
**Song Data:**
- **Title:** ${track_name}
- **Artist:** ${artist}
- **Duration (ms):** ${duration_ms}
- **Lyrics:** ${lyrics}
- **Sentiment Score:** ${sentiment_score} (Song's overall sentiment; + values indicate positive sentiment, - values indicate negative sentiment.)
- **Dominant Emotion:** ${dominant_emotion} (The primary emotion conveyed by the song's lyrics.)
- **Acousticness:** ${acousticness} (Confidence from 0.0 to 1.0 that the track is acoustic; higher values indicate more acoustic content.)
- **Danceability:** ${danceability} (How suitable the track is for dancing; 0.0 is least danceable in terms of tempo, rhythm stability, beat strength, and overall regularity; 1.0 is most danceable.)
- **Energy:** ${energy} (Intensity and activity measure; 0.0 is least energetic (such as Bach), 1.0 is most energetic (such as death metal).)
- **Instrumentalness:** ${instrumentalness} (Likelihood of the track containing no vocals; values closer to 1.0 indicate higher probability of being instrumental.)
- **Key:** ${key} (The key of the track)
- **Liveness:** ${liveness} (Detects audience presence; values above 0.8 suggest a live performance.)
- **Loudness:** ${loudness} dB (Overall track loudness in decibels)
- **Modality:** ${modality} (The modality (major or minor) of the track; 0 is minor, 1 is major.)
- **Speechiness:** ${speechiness} (Presence of spoken words; values closer to 1.0 indicate more speech-like content.)
- **Tempo:** ${tempo} BPM (Overall estimated tempo in beats per minute.)
- **Time Signature:** ${time_signature} (Estimated time signature; how many beats are in each bar.)
- **Valence:** ${valence} (Musical positiveness; 0.0 is negative emotions like sad or angry, 1.0 is positive emotions like happy or euphoric.)
"""

basic_prompt_template = string.Template(basic_prompt_template)
song_data_template = string.Template(song_data_template)

class SongAnalyzer:

    @staticmethod
    def key_mapper(key: int):
        if key == -1:
            return "Unknown"
        if key == 0:
            return "C"
        elif key == 1:
            return "C#"
        elif key == 2:
            return "D"
        elif key == 3:
            return "D#"
        elif key == 4:
            return "E"
        elif key == 5:
            return "F"
        elif key == 6:
            return "F#"
        elif key == 7:
            return "G"
        elif key == 8:
            return "G#"
        elif key == 9:
            return "A"
        elif key == 10:
            return "A#"
        elif key == 11:
            return "B"

    @staticmethod
    def clean_lyrics(lyrics: str):
        lyrics = lyrics.replace(".", ". ").replace(",", ", ").replace(")",  ") ")

        # Separate seperate words by if a letter is capitalized
        for i in range(1, len(lyrics)):
            if lyrics[i].isupper() and lyrics[i - 1] != " ":
                lyrics = lyrics[:i] + " " + lyrics[i:]
        
        return lyrics
    
    @staticmethod
    def analyze_lyrics_sentiment(lyrics: str, sentiment_pipeline, emotion_pipeline):
        if not lyrics or lyrics == "Lyrics not found.":
            return None, None

        lyrics = SongAnalyzer.clean_lyrics(lyrics)

        try:
            sentiment_result = sentiment_pipeline(lyrics[:512])[0]
            sentiment_score = sentiment_result['score'] if sentiment_result['label'] == "POSITIVE" else -sentiment_result['score']
            
            emotion_result = emotion_pipeline(lyrics[:512])[0]
            dominant_emotion = emotion_result['label']

            return sentiment_score, dominant_emotion
        except Exception as e:
            print(f"Error analyzing lyrics: {e}")
            return None, None
    
    @staticmethod
    def send_groq_request(messages: list[dict], model: str):
        # Create request
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {groq_api_key}"
        }

        # Send request
        response = requests.post(
            groq_completion_url,
            headers=headers,
            json={"messages": messages, "model": model})

        data = response.json()

        return data
    
    @staticmethod
    def convert_retry_time_to_seconds(time_str: str):
        # Regular expression to extract hours, minutes, and seconds
        time_pattern = r'(?:(?P<hours>\d+\.?\d*)h)?(?:(?P<minutes>\d+\.?\d*)m)?(?:(?P<seconds>\d+\.?\d*)s)?'
        match = re.fullmatch(time_pattern, time_str)
        
        if not match:
            raise ValueError(f"Invalid time format: {time_str}")

        # Extract the matched groups and convert them to float if they exist
        hours = float(match.group('hours')) if match.group('hours') else 0.0
        minutes = float(match.group('minutes')) if match.group('minutes') else 0.0
        seconds = float(match.group('seconds')) if match.group('seconds') else 0.0

        # Calculate total seconds
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds

    @staticmethod
    def analyze_song_fits_groq(songs: list[dict], playlist_description: str):
        basic_prompt = basic_prompt_template.substitute({"playlist_description": playlist_description})
        messages = [{"role": "user", "content": basic_prompt}]

        for song in songs:
            substitute_dict = {}
            substitute_dict.update(song)

            song_data_prompt = song_data_template.substitute(substitute_dict)

            messages.append({
                "role": "user",
                "content": f"\n\n{song_data_prompt}"
            })

        # model = groq_models[0]
        # data = SongAnalyzer.send_groq_request(messages, model)

        # Use a pipeline as a high-level helper
        # pipe = pipeline("text-generation", model="mistralai/Mixtral-8x7B-Instruct-v0.1")
        # pipe(messages)
        # Load model directly
        from transformers import AutoTokenizer, AutoModelForCausalLM

        tokenizer = AutoTokenizer.from_pretrained("mistralai/Mixtral-8x7B-Instruct-v0.1")
        model = AutoModelForCausalLM.from_pretrained("mistralai/Mixtral-8x7B-Instruct-v0.1")

        inputs = tokenizer(messages, return_tensors="pt")
        outputs = model(**inputs)
        print(outputs)
        
        sys.exit()

        while 'error' in data.keys():
            error_message = data['error']['message']
            # Check if "Please try again in X.XXs." is in message
            if 'try again in' in error_message:
                retry_after = error_message.split('try again in ')[1].split('s')[0] + "s"
                
                logging.warning(f"Groq API rate limit exceeded for llama-3.1-8b-instant. Retrying in {retry_after}...")
                
                retry_after = SongAnalyzer.convert_retry_time_to_seconds(retry_after)
                
                logging.info(f"Sleeping for {retry_after} seconds...")
                time.sleep(float(retry_after) + 1)

                data = SongAnalyzer.send_groq_request(messages)

        response_message = data['choices'][0]['message']['content']

        # Extract results from response by finding each occurrence of YUP and NOPE
        yups_and_nopes = {}
        yup_nope_index = 0

        # Find each YUP and NOPE plus the explanation for each
        lines = response_message.split("\n")
        for line in lines:

            if len(yups_and_nopes.keys()) >= len(songs):
                break

            line_index = lines.index(line)
            match = re.search(r"YUP|NOPE", line)
            if not match:
                continue

            # Explanation is the next lines until an empty line after the YUP/NOPE line
            explanation = ""
            explanation_index = line_index + 1
            while explanation_index < len(lines) and lines[explanation_index] != "":
                explanation += lines[explanation_index] + "\n"
                explanation_index += 1

            # Store YUP and NOPE
            yups_and_nopes[yup_nope_index] = {"result": line, "explanation": explanation}

            yup_nope_index += 1

        return yups_and_nopes
