import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Tải biến môi trường
load_dotenv()
cid = os.getenv('CLIENT_ID')
secret = os.getenv('CLIENT_SECRET')

# Xác thực với Spotify API
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Danh sách chứa tất cả các bài hát
all_tracks = []

# Giới hạn số lượng bài hát mỗi lần lấy
limit = 50
offset = 0

while True:
    results = sp.search(q='year:2024', type='track', limit=limit)
    tracks = results['tracks']['items']
    
    if not tracks or offset == 200:
        break
    
    all_tracks.extend(tracks)
    offset += limit  # Tăng offset để lấy bài hát tiếp theo

# In danh sách bài hát
    for track in all_tracks:
        print(f"Track: {track['name']} - Artist: {track['artists'][0]['name']}")
