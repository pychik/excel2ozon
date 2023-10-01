from pydantic import BaseSettings


class Settings(BaseSettings):
    INVASK_API_TOKEN: str
    INVASK_API_URL: str
    FTP_HOST: str = "46.254.21.136"
    FTP_USER: str
    FTP_PASSWORD: str
    OZON_API_KEY: str
    OZON_MAX_ITEMS: int
    OZON_MIN_ITEMS: str
    OZON_CLIENT_ID: str
    OZON_STOCK_URL: str
    OZON_STOCK_UPDATE_URL: str
    OZON_PRICE_UPDATE_URL: str
    OZON_WAREHOUSE_ID: int = 22053606930000
    PRICE_URL: str
    TABLE_URL: str
    SHEET_NAME: str = "Sheet1"
    TABLE_NAME: str = "table_invask.xlsx"
    UPLOAD_TEMPLATE_YM: str = 'upload_template'
    UPLOAD_NAME_YM: str = 'yandex_upload.xlsx'
    UPLOAD_YM_TABLE_WORKSHEET: str = 'Остатки'
    UPDATE_PERIOD: int = 3600
    TIME_EXECUTE: str
    LEN_TOKEN: int = 40
    SIMA_PHONE: str
    SIMA_PASS: str
    SIMA_LAND_URL: str = "https://www.sima-land.ru/api/v5/"

    class Messages:
        START_MESSAGE: str = "Начат процесс обновления таблицы остатков склада!"
        STOCK_UPLOADED: str = "Информация об остатках загружена!"
        TABLE_CREATED: str = "Таблица для Yandex market создана!"
        TABLE_UPLOADED: str = "Таблицы загружена на хостинг!"
        PROCESS_COMPLETE: str = "Процесс обновления таблицы завершен!"

    class Prices:
        TABLEFILE: str = 'table_invask.xlsx'
        DELIVERY: int = 500
        AC: str = "B"
        PC: str = "D"
        START_ROW: int = 2

    class Config:
        env_file = '.env'


settings = Settings()
