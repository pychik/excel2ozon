from pydantic import BaseSettings


class Settings(BaseSettings):
    USER_LOGIN: str
    USER_PASSWORD: str
    LOGIN_URL: str
    OZON_API_KEY: str
    OZON_MAX_ITEMS: int
    OZON_MIN_ITEMS: str
    OZON_CLIENT_ID: str
    OZON_STOCK_URL: str
    OZON_STOCK_UPDATE_URL: str
    OZON_WAREHOUSE_ID: int = 22053606930000
    PRICE_URL: str
    TABLE_URL: str
    TABLE_NAME: str = "table.xlsx"
    TIME_EXECUTE: str
    LEN_TOKEN: int = 40


    class Config:
        env_file = '.env'


settings = Settings()
