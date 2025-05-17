import logging
from itertools import batched

from src.clouder_beats.bp_adapter import BPItemType, fetch_bp_items
from src.clouder_beats.config import settings
from src.clouder_beats.mongo_adapter import get_data, save_data_mongo_by_id
from src.clouder_beats.sp_adapter import (
    add_tracks_to_playlist,
    create_playlist,
    get_track_by_isrc,
)
from src.clouder_beats.statistics import StatisticEnum, track_statistics
from src.clouder_beats.week_harvest import WeekHarvest

logger = logging.getLogger("collectors")


def save_clouder_week(week_harvest: WeekHarvest):
    save_data_mongo_by_id(week_harvest.data_to_mongo(), "clouder_weeks", ["week"])


@track_statistics(StatisticEnum.BEATPORT)
def collect_bp_items(week_harvest: WeekHarvest, bp_item_type: BPItemType) -> dict:
    logger.info(f"Collecting {bp_item_type.value} for {week_harvest} :: Starting")
    items = fetch_bp_items(week_harvest, bp_item_type)
    statistic = {
        "full_cnt": 0,
        "inserted": 0,
        "updated": 0,
    }
    for chunk in batched(items, settings.bp_chunk_size):
        statistic["full_cnt"] += len(chunk)
        for el in chunk:
            el["clouder_week"] = week_harvest.clouder_week
        try:
            chunk_inserted, chunk_updated = save_data_mongo_by_id(
                chunk, f"bp_{bp_item_type.value}", key_fields=["id", "clouder_week"]
            )
            statistic["inserted"] += chunk_inserted
            statistic["updated"] += chunk_updated
        except Exception as e:
            logger.error(f"Failed to save BP {bp_item_type.value} :: {e}")
            raise
    logger.info(f"{week_harvest} Saved {bp_item_type.value} :: {statistic}")
    return statistic


def collect_bp_tracks(week_harvest: WeekHarvest):
    collect_bp_items(week_harvest, bp_item_type=BPItemType.TRACK)


@track_statistics(StatisticEnum.SPOTIFY)
def collect_sp_tracks(week_harvest: WeekHarvest):
    logger.info(f"Collecting Spotify tracks for {week_harvest} :: Starting")
    filters = {"clouder_week": week_harvest.clouder_week}
    fields = ["id", "isrc", "genre.id"]
    bp_tracks = get_data("bp_tracks", filters, fields)
    full_cnt, found, is_genre = len(bp_tracks), 0, 0
    sp_tracks = []
    for bp_track in bp_tracks:
        sp_track = get_track_by_isrc(bp_track["isrc"])
        if sp_track:
            if "available_markets" in sp_track:
                sp_track.pop("available_markets", None)
            album_info = sp_track["album"]
            if "available_markets" in album_info:
                album_info.pop("available_markets", None)
            sp_track["bp_id"] = bp_track["id"]
            sp_track["clouder_week"] = week_harvest.clouder_week
            if "genre" in bp_track:
                sp_track["bp_genre_id"] = bp_track["genre"]["id"]
                if sp_track["bp_genre_id"] == week_harvest.style_id:
                    is_genre += 1
            sp_tracks.append(sp_track)
            found += 1
            if len(sp_tracks) % 10 == 0:
                logger.info(
                    f"{week_harvest} Got Spotify tracks :: {found} / {full_cnt}"
                )

    save_data_mongo_by_id(sp_tracks, "sp_tracks", key_fields=["id", "clouder_week"])

    statistics = {
        "full_cnt": full_cnt,
        "found": found,
        "not_found": full_cnt - found,
        "is_genre": is_genre,
        "not_genre": found - is_genre,
    }
    logger.info(f"{week_harvest} Got Spotify tracks :: {statistics}")
    return statistics


def create_sp_playlists(week_harvest: WeekHarvest):
    logger.info(f"Collecting Spotify playlists for {week_harvest} :: Starting")
    sp_playlists = []
    exists_playlists = get_data(
        "sp_playlists", {"clouder_week": week_harvest.clouder_week}
    )
    if exists_playlists:
        logger.warning(
            f"{week_harvest} Spotify playlists already exists"
        )
        return
    for pl_type, pl_names in week_harvest.playlists.items():
        for pl_name in pl_names:
            sp_name = week_harvest.generate_sp_playlist_name(pl_name)
            sp_playlist_id = create_playlist(sp_name)
            if sp_playlist_id:
                sp_playlists.append(
                    {
                        "clouder_week": week_harvest.clouder_week,
                        "playlist_id": sp_playlist_id,
                        "playlist_name": sp_name,
                        "clouder_pl_type": pl_type,
                        "clouder_pl_name": pl_name,
                    }
                )
            else:
                logger.error(f"Failed to create Spotify playlist :: {sp_name}")
    save_data_mongo_by_id(sp_playlists, "sp_playlists", key_fields=["playlist_id"])
    logger.info(f"{week_harvest} Got Spotify playlists :: {len(sp_playlists)}")


def populate_one_sp_pl(
    week_harvest: WeekHarvest, pl_type: str, track_filters: dict
) -> int:
    logger.info(f"Populating Spotify playlists for {week_harvest} :: Starting")
    pl_filters = {"clouder_week": week_harvest.clouder_week, "clouder_pl_name": pl_type}
    pl_fields = ["playlist_id"]
    sp_playlists = get_data("sp_playlists", pl_filters, pl_fields)
    playlist_id = sp_playlists[0]["playlist_id"] if sp_playlists else None
    if not playlist_id:
        raise ValueError(f"Spotify playlist not found for '{pl_type}'")

    query_sort = None
    if week_harvest.style_id != 1:
        track_filters["popularity"] = {"$gt": 0}
        query_sort = [("popularity", -1)]
    sp_tracks = get_data("sp_tracks", track_filters, ["uri"], query_sort)
    if sp_tracks:
        uris = [track["uri"] for track in sp_tracks]
        add_tracks_to_playlist(playlist_id, uris)
        logger.info(
            f"{week_harvest} Populated Spotify playlist '{pl_type}' :: {len(uris)}"
        )
        return len(uris)
    else:
        logger.warning(f"{week_harvest} Spotify tracks not found for '{pl_type}'")
        return 0


@track_statistics(StatisticEnum.SP_PLAYLIST)
def populate_sp_playlists(week_harvest: WeekHarvest):
    filters_new = {
        "clouder_week": week_harvest.clouder_week,
        "bp_genre_id": week_harvest.style_id,
        "album.release_date": {"$gte": week_harvest.sp_week_start},
    }
    new_cnt = populate_one_sp_pl(week_harvest, "new", filters_new)

    filters_old = {
        "clouder_week": week_harvest.clouder_week,
        "bp_genre_id": week_harvest.style_id,
        "album.release_date": {"$lt": week_harvest.sp_week_start},
    }
    old_cnt = populate_one_sp_pl(week_harvest, "old", filters_old)

    filters_not = {
        "clouder_week": week_harvest.clouder_week,
        "bp_genre_id": {"$ne": week_harvest.style_id},
    }
    not_cnt = populate_one_sp_pl(week_harvest, "not", filters_not)
    return {
        "new": new_cnt,
        "old": old_cnt,
        "not": not_cnt,
    }


def handle_clouder_week(week_harvest: WeekHarvest):
    logger.info(f"Processing week {week_harvest} :: Starting")
    save_clouder_week(week_harvest)
    collect_bp_tracks(week_harvest)
    collect_sp_tracks(week_harvest)
    create_sp_playlists(week_harvest)
    populate_sp_playlists(week_harvest)
    logger.info(f"Processing week {week_harvest} :: Done")
