import requests
import logging as logger
import pandas as pd
from datetime import datetime
from io import BytesIO
from sys import exit as s_exit
from time import sleep, time
from typing import Generator

from config import settings
logger.basicConfig(level=logger.INFO, format="%(asctime)s %(levelname)s %(message)s")


class TableGetter:
    def __init__(self, login_url: str, price_url: str) -> None:
        self.login_url = login_url
        self.price_url = price_url

    def register_download(self) -> requests.models.Response:
        with requests.Session() as s:
            g_resp = s.get(url=self.login_url)
            search_string = g_resp.text

            # Searching CSRF token
            start_phrase = '<input type="hidden" name="_token" value="'
            start = search_string.find(start_phrase) + len(start_phrase)
            token = search_string[start:start + settings.LEN_TOKEN]

            payload = {
                '_token': token,
                'email': settings.USER_LOGIN,
                'password': settings.USER_PASSWORD,
                'remember': 0,
            }
            p = s.post(url=self.login_url, data=payload)
            if p.status_code != 200:
                logger.warning(msg="Не удалось авторизоваться, проверьте правильность вводимых данных")
                s_exit()
            logger.info(msg=f"Успешная авторизация на сайте {settings.LOGIN_URL} с токеном {token}")

            table_response = s.get(url=settings.TABLE_URL)
            if table_response.status_code != 200:
                logger.warning(msg="Таблицу не удалось скачать. Обратитесь к разработчику!")
                s_exit()
            logger.info(msg="Таблица скачана в буфер обмена и готова к обработке")
            return table_response

    @staticmethod
    def process_table(table) -> pd.core.frame.DataFrame:
        with BytesIO(table.content) as fh:
            df = pd.io.excel.read_excel(fh, sheet_name=0,).loc[13:]

            # remove all rows with blank article
            df = df[df.iloc[:, 0] != ''].dropna()

            # select columns with article and quantity
            df = df.iloc[:, [0, 8]]
            df = df.replace("> 10", settings.OZON_MAX_ITEMS).replace(eval(settings.OZON_MIN_ITEMS), 0)
            df.rename(columns={df.columns[0]: 'offer_id', df.columns[1]: 'stock_val'}, inplace=True)

            items_quantity = len(df.index)
            logger.info(msg=f"Таблица обработана, содержит {items_quantity} записей")

            return df


class OzonApi:
    def __init__(self, client_id: str, api_key: str) -> None:
        self.client_id = client_id
        self.api_key = api_key
        self.last_id = None
        self.res_list = []

    def get_stock_items(self):
        while self.last_id != '':
            self.res_list += self.get_stock_items_batch()
        logger.info(msg="Данные с озон склада получены")
        return self.res_list

    def get_stock_items_batch(self):
        with requests.Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}
            payload = dict(last_id=self.last_id) if self.last_id else dict()
            response = s.post(url=settings.OZON_STOCK_URL, headers=headers, json=payload)
            res_dict = response.json()

            last_id = res_dict.get("result").get("last_id")
            print(last_id)
            if last_id == '':
                self.last_id = last_id
                return []
            self.last_id = last_id
            batch_list = res_dict.get("result").get('items')
            if not batch_list:
                logger.warning(msg=f"Во время выгрузки данных товаров на озон произошла ошибка {res_dict}")
                s_exit()
            return batch_list

    def process_stock_items(self, stock_list: list, df_site: pd.core.frame.DataFrame) -> tuple:
        df_stock_raw = pd.DataFrame(stock_list)
        df_stock = df_stock_raw[['product_id', 'offer_id']]
        df_stock_quants = pd.DataFrame(columns=['offer_id', 'product_id', 'stock', 'warehouse_id'])

        stock_quants = []
        for index, row in df_stock[["product_id", "offer_id"]].iterrows():
            key_oid = row["offer_id"]
            key_pid = row["product_id"]

            # get stock value from supplier table
            stock_value = tuple(df_site[df_site['offer_id'] == key_oid]['stock_val'])
            if len(stock_value) == 0:
                continue
            stock_quants.append(dict(offer_id=key_oid,
                                     product_id=key_pid,
                                     stock=stock_value[0],
                                     warehouse_id=settings.OZON_WAREHOUSE_ID))

        df_stock_quants = pd.concat([df_stock_quants, pd.DataFrame(stock_quants)])
        result_list_dicts = df_stock_quants.to_dict(orient="records")
        batch_list = self.list_batcher(list_dicts=result_list_dicts)
        logger.info(msg=f"Завершено сопоставление таблицы артикулов и таблицы поставщика.\n"
                        f"Обработано {len(result_list_dicts)} записей")
        return batch_list, len(result_list_dicts)

    def update_stock(self, list_send: Generator, len_list: int):
        with requests.Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}

            for el in list_send:
                payload = dict(stocks=el)
                response = s.post(url=settings.OZON_STOCK_UPDATE_URL, headers=headers, json=payload)
                logger.info(msg=f"Пачка данных обработана:{response.json()}")
                if len_list > 8000:
                    sleep(1)

    @staticmethod
    def list_batcher(list_dicts: list, n: int = 100):
            len_list = len(list_dicts)
            for ndx in range(0, len_list, n):
                yield list_dicts[ndx:min(ndx + n, len_list)]


def runner():
    start = time()
    gt = TableGetter(login_url=settings.LOGIN_URL, price_url=settings.PRICE_URL)
    table_resp = gt.register_download()
    df_orig = gt.process_table(table=table_resp)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY)
    stock_list = oa.get_stock_items()
    batches2send, len_list = oa.process_stock_items(stock_list, df_site=df_orig)
    oa.update_stock(list_send=batches2send, len_list=len_list)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада выполнено за {delta}")


if __name__ == '__main__':
    exec_hour, exec_min = tuple(settings.TIME_EXECUTE.split(":"))
    if int(exec_hour) == 0 or exec_hour == 23:
        logger.warning(msg="выберите время запуска с 01 до 22. Обработчик выклается")
        s_exit()
    # Preare our schedule hour list for sleeping until exec time
    scheduler_list = [i for i in range(24)]
    exec_val_index = scheduler_list.index(int(exec_hour))
    del scheduler_list[exec_val_index-1]
    del scheduler_list[exec_val_index-1]
    del scheduler_list[exec_val_index-1]

    while True:
        time_pc = datetime.now()
        time_pc_hour, time_pc_min = time_pc.strftime("%H"), time_pc.strftime("%M")

        if time_pc_hour == exec_hour:
            logger.info(msg=f"{time_pc} - запускаю обработчик")
            runner()
            logger.info(msg=f"Обработчик запустится через сутки - а пока баиньки")
            sleep(7200)
        if int(time_pc_hour) in scheduler_list:
            logger.info(msg=f"{time_pc} время для запуска еще не настало")
            sleep(3600)
        else:
            logger.info(msg=f"{time_pc} скоро запустится обработчик")
            sleep(60)




