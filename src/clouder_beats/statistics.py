import logging
from enum import Enum
from functools import wraps

from src.clouder_beats.mongo_adapter import save_data_mongo_by_id
from src.clouder_beats.week_harvest import WeekHarvest

logger = logging.getLogger("main")


class StatisticEnum(Enum):
    BEATPORT = "beatport"
    SPOTIFY = "spotify"
    SP_PLAYLIST = "sp_playlist"


def track_statistics(stat_type: StatisticEnum):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            stat_name = stat_type.value
            week_harvest: WeekHarvest = kwargs.get("week_harvest") or args[0]
            if stat_type == StatisticEnum.BEATPORT:
                bp_item_type = kwargs.get("bp_item_type") or args[1]
                stat_name += f"_{bp_item_type.value}"
            result = func(*args, **kwargs)
            stat = {"id": week_harvest.clouder_week, stat_name: result}
            try:
                save_data_mongo_by_id([stat], "statistics")
            except Exception as e:
                logger.error(f"Failed to save {stat_name} statistics :: {e}")
            return result

        return wrapper

    return decorator
