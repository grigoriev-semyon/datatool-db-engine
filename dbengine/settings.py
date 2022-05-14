from pydantic import BaseSettings


class Settings(BaseSettings):
    DB_DSN: str

    class Config:
        case_sensitive = True
        env_file = ".env"
