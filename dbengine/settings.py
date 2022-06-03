from pydantic import BaseSettings, AnyUrl


class Settings(BaseSettings):
    DB_DSN: AnyUrl
    DWH_CONNECTION_TEST: AnyUrl
    DWH_CONNECTION_PROD: AnyUrl

    class Config:
        case_sensitive = True
        env_file = ".env"
