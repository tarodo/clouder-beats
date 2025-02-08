import os

import typer
from pydantic_settings import BaseSettings


def get_bp_token(is_new: bool = False) -> str:
    """
    Gets the Beatport API token.
    """
    if not is_new and os.path.exists(".bp_cache"):
        with open(".bp_cache") as f:
            return f.read()
    else:
        token = typer.prompt("Enter your Beatport API token", hide_input=True)
        with open(".bp_cache", "w") as f:
            f.write(token)
        return token


class AppSettings(BaseSettings):
    env: str = "dev"
    log_level: str = "INFO"
    bp_api_url: str
    bp_api_token: str = get_bp_token()
    bp_chunk_size: int = 100
    mongo_url: str
    mongo_db: str
    spotipy_client_id: str
    spotipy_client_secret: str
    spotipy_redirect_uri: str

    class Config:
        env_file = ".env"


settings = AppSettings()
