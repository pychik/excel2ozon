from pydantic import BaseSettings


class Settings(BaseSettings):
    OZON_API_KEY: str
    OZON_MAX_ITEMS: int
    OZON_MIN_ITEMS: str
    OZON_CLIENT_ID: str
    OZON_STOCK_URL: str
    OZON_WAREHOUSE_ID: str
    OZON_STOCK_UPDATE_URL: str
    OZON_PRICE_UPDATE_URL: str
    RUSKLIMAT_LOGIN: str
    RUSKLIMAT_PASSWORD: str
    RUSKLIMAT_URL_JWT: str
    RUSKLIMAT_URL_RQ: str
    RUSKLIMAT_URL_DATA: str
    UPDATE_PERIOD: int
    PRICE_TABLE: str
    ARTICLE_COLUMN: str
    PRICE_COLUMN: str
    START_ROW: int

    class Prices:
        PRICE_TABLE: str
        ARTICLE_COLUMN: str
        PRICE_COLUMN: str
        START_ROW: int
        DELIVERY: int = 500

    class Messages:
        START_MESSAGE: str = "Начат процесс обновления таблицы остатков склада!"
        STOCK_UPLOADED: str = "Информация об остатках загружена!"
        TABLE_CREATED: str = "Таблица для Yandex market создана!"
        TABLE_UPLOADED: str = "Таблицы загружена на хостинг!"
        PROCESS_COMPLETE: str = "Процесс обновления таблицы завершен!"

    class Config:
        env_file = '.env'
    # pyinstaller --onefile --exclude-module python-dotenv  main.py


settings = Settings()
