from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables or defaults."""

    app_name: str = "StreamGuard AI"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_timeout_sec: float = 1.5
    simulation_fallback: bool = True
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    runbooks_dir: Path = Path(__file__).resolve().parent.parent / "runbooks"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
