"""
Beatport API adapter module.

This module provides functionality to interact with the Beatport API,
including making requests and collecting releases or tracks for a given week.
It handles pagination and error cases when communicating with the API.

Classes:
    ReleaseType: Enum for different types of items that can be retrieved

Functions:
    request_bp_api: Makes a single request to the Beatport API
    collect_bp_items: Collects all items from Beatport API for a given week
    collect_release_tracks: Collects all tracks for a specific release
        from the Beatport API
"""

import logging
import time
from collections.abc import Generator
from enum import Enum

import requests

from src.clouder_beats.config import get_bp_token, settings
from src.clouder_beats.week_harvest import WeekHarvest

logger = logging.getLogger("bp")


class BPItemType(Enum):
    RELEASE = "releases"
    TRACK = "tracks"


def request_bp_api(url: str, params: dict) -> tuple[list, str, dict, bool]:
    """
    Requests the Beatport API.
    """
    logger.info(f"Requesting {url} with params {params}")
    if not url.startswith("https://"):
        url = f"https://{url}"
    headers = {
        "Authorization": f"Bearer {settings.bp_api_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 401:
        new_token = get_bp_token(True)
        settings.bp_api_token = new_token
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], url, params, True
    one_page = response.json()
    next_page = one_page["next"]
    cur_page = one_page["page"]
    full_count = one_page["count"]
    logger.info(
        f"Got {len(one_page['results'])} results on page {cur_page} of {full_count}"
    )
    return one_page["results"], next_page, dict(), False


def fetch_bp_items(
    week_harvest: WeekHarvest, bp_item_type: BPItemType
) -> Generator[dict]:
    """
    Collects items from Beatport API for a given week and release type.

    Args:
        week_harvest: WeekHarvest object containing week and style information
        bp_item_type: Type of items to collect (releases or tracks)

    Yields:
        Individual items from the API response
    """
    logger.info(f"Collecting {bp_item_type.value} for {week_harvest} :: Starting")

    params = {
        "genre_id": week_harvest.style_id,
        "publish_date": f"{week_harvest.week_start}:{week_harvest.week_end}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }

    url = f"{settings.bp_api_url}/{bp_item_type.value}/"

    while url:
        items, url, params, failed = request_bp_api(url, params)
        yield from items

    logger.info(f"Collecting {bp_item_type.value} for {week_harvest} :: Done")


def fetch_release_tracks(release_id: str, token: str) -> Generator[dict]:
    """
    Collects all tracks for a specific release from the Beatport API.

    Args:
        release_id: ID of the release to get tracks for
        token: Beatport API token

    Yields:
        Individual track items from the API response
    """
    logger.info(f"Collecting tracks for release {release_id} :: Starting")

    url = f"{settings.bp_api_url}/releases/{release_id}/tracks/"
    params = {
        "page": 1,
        "per_page": 100,
    }

    max_retries = 3
    retry_delay = 1

    while url:
        for _ in range(max_retries):
            tracks, url, params, failed = request_bp_api(url, params, token)
            if not failed:
                break
            time.sleep(retry_delay)
        else:
            logger.error(f"Failed to get tracks for release {release_id}")
            break

        yield from tracks

    logger.info(f"Collecting tracks for release {release_id} :: Done")
