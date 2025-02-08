from datetime import date, datetime, timedelta

from dateutil.relativedelta import MO, SU, relativedelta

STYLES = {
    1: "dnb",
    90: "techno",
}

PLAYLISTS = {
    "base": ["new", "old", "not", "trash"],
    "dnb": ["melodic", "eastern", "hard", "shadowy", "party", "redrum", "alt"],
    "techno": ["mid", "eastern", "house", "low", "up", "alt"],
}

BASE_PLAYLIST = "base"


class WeekHarvest:
    def __init__(self, week: int, year: int, style_id: int):
        """
        Initializes the WeekHarvest object with week, year, and style ID.
        Calculates the start and end dates for the given week.
        """
        self._week = week
        self._year = year

        if style_id not in STYLES:
            raise ValueError(f"Style ID {style_id} is not recognized.")
        self._style_id = style_id
        self._style_name = STYLES[style_id]

        self._week_start, self._week_end = self.get_start_end_dates(year, week)

    @staticmethod
    def get_start_end_dates(year: int, week_number: int) -> tuple[date, date]:
        """
        Returns the start and end dates for the given week in the specified year.
        Raises an error if the week number is out of bounds.
        """
        first_day = datetime(year, 1, 1).date()

        if first_day.weekday() > 0:
            first_day += relativedelta(weekday=MO(1))

        start_date = first_day + relativedelta(weeks=week_number - 1)
        end_date = start_date + relativedelta(weekday=SU(1))

        if start_date.year != year:
            raise ValueError(
                f"Week number {week_number} is out of bounds for the year {year}."
            )

        return start_date, end_date

    @property
    def week_start(self) -> str:
        """Returns the start date of the week as an ISO-formatted string."""
        return self._week_start.isoformat()

    @property
    def sp_week_start(self) -> str:
        return (self._week_start - timedelta(days=7)).isoformat()

    @property
    def week_end(self) -> str:
        """Returns the end date of the week as an ISO-formatted string."""
        return self._week_end.isoformat()

    @property
    def style_name(self) -> str:
        """Returns the style name based on the style ID."""
        return self._style_name

    @property
    def style_id(self) -> int:
        """Returns the style ID."""
        return self._style_id

    @property
    def year(self) -> int:
        """Returns the year of the week."""
        return self._year

    @property
    def playlists(self) -> dict[str, list[str]]:
        """
        Returns all playlists, including base and categorized ones.
        """
        return {
            "base": PLAYLISTS[BASE_PLAYLIST],
            "category": PLAYLISTS.get(self._style_name, []),
        }

    @property
    def _base_sp_pl_name(self) -> str:
        """Generates the base name for the specialized playlist."""
        return f"{self._style_name.upper()} :: {self._year} :: {self._week:02d}"

    def generate_sp_playlist_name(self, pl_name: str) -> str:
        """Generates a formatted name for a specialized playlist."""
        return f"{self._base_sp_pl_name} :: {pl_name.upper()}"

    @property
    def clouder_week(self) -> str:
        """Returns a unique identifier for the week in uppercase format."""
        return f"{self._style_name}_{self._year}_{self._week}".upper()

    def __str__(self) -> str:
        """Returns the string representation of the ReleaseMeta object."""
        return self.clouder_week

    def data_to_mongo(self):
        return [
            {
                "week": self._week,
                "year": self._year,
                "style": self.style_name,
                "style_id": self._style_id,
                "week_start": self._week_start.isoformat(),
                "week_end": self._week_end.isoformat(),
                "id": self.clouder_week,
                "base_playlists": PLAYLISTS[BASE_PLAYLIST],
                "cat_playlists": PLAYLISTS.get(self._style_name, []),
            },
        ]
