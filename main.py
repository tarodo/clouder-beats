import logging

from src.clouder_beats.collectors import handle_clouder_week
from src.clouder_beats.config import settings
from src.clouder_beats.logging_config import setup_logging
from src.clouder_beats.week_harvest import WeekHarvest

if settings.env == "dev":
    from dotenv import load_dotenv

    load_dotenv()

setup_logging()
logger = logging.getLogger("main")


def main():
    active_week = WeekHarvest(7, 2025, 1)
    handle_clouder_week(active_week)


if __name__ == "__main__":
    main()
