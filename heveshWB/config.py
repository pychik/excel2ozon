from pydantic import BaseSettings


class Settings(BaseSettings):
    WB_API_KEY: str
    WB_STOCK_URL: str
    WB_STOCK_UPDATE_URL: str
    EXCEL_FILE: str
    EXCEL_SHEET_NAME: str
    EXCEL_OFFER_ID_COL: str
    EXCEL_QUANTITY_COL: str
    START_TIME: str
    STOP_TIME: str
    WB_WAREHOUSE_ID: int
    UPDATE_PERIOD: int

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
