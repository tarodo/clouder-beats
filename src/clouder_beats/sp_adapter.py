import logging
from itertools import batched

from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

logger = logging.getLogger("sp")


def create_sp():
    scope = "playlist-modify-public playlist-modify-private"
    return Spotify(
        auth_manager=SpotifyOAuth(scope=scope, open_browser=False, show_dialog=True)
    )


def get_track_by_isrc(isrc: str) -> dict | None:
    sp = create_sp()
    track_result = sp.search(q=f"isrc:{isrc}", type="track", limit=1)
    tracks = track_result["tracks"]["items"]
    if tracks:
        sp_track = tracks[0]
        return sp_track
    return None


def create_playlist(title: str) -> str:
    sp = create_sp()
    user_id = sp.me()["id"]
    playlist = sp.user_playlist_create(user_id, title, public=False)
    logger.info(f"Playlist created : {playlist['id']} : {playlist['name']}")
    return playlist["id"]


def add_tracks_to_playlist(playlist_id: str, tracks_ids: list[str]):
    logger.info(
        f"Tracks added to playlist : {playlist_id} : {len(tracks_ids)} :: Start"
    )
    sp = create_sp()
    for part in batched(tracks_ids, 100):
        sp.playlist_add_items(playlist_id, list(part))
        logger.info(f"Tracks added to playlist : {playlist_id} : {len(part)}")
    logger.info(f"Tracks added to playlist : {playlist_id} : {len(tracks_ids)} :: Done")
    return True
