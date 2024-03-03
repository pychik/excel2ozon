# import requests
from requests import Session
import logging as logger
import pandas as pd
from datetime import datetime
from sys import exit as s_exit
from time import sleep, time
from typing import Generator

from config import settings
logger.basicConfig(level=logger.INFO, format="%(asctime)s %(levelname)s %(message)s")


class TableGetter:

    @staticmethod
    def table_requester():
        with Session() as s:
            response = s.get(url=settings.ARMAVIR_URL)
            if not response.status_code == 200:
                logger.warning(msg=f"Во время загрузки данных с {settings.ARMAVIR_URL} произошла ошибка\n"
                                   f"Ответ сервера: {response.status_code} \n {response.text}")
                s_exit()
            res_list = response.json()
            return res_list

    @staticmethod
    def process_table(product_list: list) -> pd.core.frame.DataFrame:

        super_list = list(
            map(lambda x: {"offer_id": str(x[0]),
                           "stock_val": str(int(float(x[1])))}, product_list))

        df_arm = pd.DataFrame(super_list)
        df_arm = df_arm.replace(eval(settings.OZON_MIN_ITEMS), 0)
        return df_arm


class OzonApi:
    def __init__(self, client_id: str, api_key: str, prices_delta_dict: dict) -> None:
        self.client_id = client_id
        self.api_key = api_key
        self.last_id = None
        self.res_list = []
        self.prices_dd = prices_delta_dict

    def get_stock_items(self):
        while self.last_id != '':
            self.res_list += self.get_stock_items_batch()
        logger.info(msg="Данные с озон склада получены")
        return self.res_list

    def get_stock_items_batch(self):
        with Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}
            payload = dict(last_id=self.last_id) if self.last_id else dict()
            response = s.post(url=settings.OZON_STOCK_URL, headers=headers, json=payload)
            res_dict = response.json()
            result = res_dict.get("result")

            if not result:
                logger.warning(msg=f"Во время выгрузки данных товаров на озон произошла ошибка {res_dict}")
                s_exit()
            else:
                last_id = result.get("last_id")
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
        # print(result_list_dicts)
        batch_list = self.list_batcher(list_dicts=result_list_dicts)
        logger.info(msg=f"Завершено сопоставление таблицы артикулов и таблицы поставщика.\n"
                    f"Обработано {len(result_list_dicts)} записей для кол-ва товаров")
        return batch_list, len(result_list_dicts)

    def update_stock(self, list_send: Generator, len_list: int, price_flag: bool = False):
        with Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}
            if not price_flag:
                for el in list_send:

                    payload = dict(stocks=el)
                    response = s.post(url=settings.OZON_STOCK_UPDATE_URL, headers=headers, json=payload)
                    logger.info(msg=f"Пачка данных кол-ва товаров обработана:{response.json()}")
            else:
                for el in list_send:
                    payload = dict(prices=el)
                    response = s.post(url=settings.OZON_PRICE_UPDATE_URL, headers=headers, json=payload)
                    logger.info(msg=f"Пачка данных цен обработана:{response.json()}")
                if len_list > 8000:
                    sleep(1)

    @staticmethod
    def list_batcher(list_dicts: list, n: int = 100):
        len_list = len(list_dicts)
        for ndx in range(0, len_list, n):
            yield list_dicts[ndx:min(ndx + n, len_list)]


def runner_stock():
    start = time()

    product_list = TableGetter.table_requester()

    df_arm = TableGetter.process_table(product_list=product_list)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict={ })
    stock_list = oa.get_stock_items()

    batches2send, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_arm)

    oa.update_stock(list_send=batches2send, len_list=len_list)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада выполнено за {delta}")


def main_proc():
    while True:
        time_pc = datetime.now()
        logger.info(msg=f"{time_pc} - запускаю обработчик")
        runner_stock()
        logger.info(msg=f"Обработчик запустится через {settings.UPDATE_PERIOD} сек - а пока баиньки")
        sleep(settings.UPDATE_PERIOD)


if __name__ == '__main__':
    # main_proc()
    # res_list = TableGetter.table_requester()
    # print(res_list)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict={})
    stock_list = oa.get_stock_items()
    print(list(map(lambda x: x.get('offer_id'), stock_list)), len(stock_list))

    #
    # with Session() as s:
    #     headers = {'Api-Key': settings.AVTO_EVRO_API_KEY}
    #     # payload = dict(code='CHB228')
    #     # response = s.post(url=settings.AVTO_EVRO_URL, headers=headers, json=payload)
    #     # response = s.post(url=settings.AVTO_EVRO_URL, headers=headers)
    #
    #     params = dict(key=settings.AVTO_EVRO_API_KEY, code='CHB228')
    #     response = s.get(url=settings.AVTO_EVRO_URL, params=params)
    #     if not response.status_code == 200:
    #         logger.warning(msg=f"Во время загрузки данных с {settings.AVTO_EVRO_URL} произошла ошибка\n"
    #                            f"Ответ сервера: {response.status_code} \n {response.text}")
    #         s_exit()
    #     res_list = response.text
    #     # res_dict = response.json()
    # print(res_list)
