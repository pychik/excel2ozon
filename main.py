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
    def __init__(self, api_token: str) -> None:
        self.api_token = api_token
        self.last_id = ''

    def table_requester(self, offset: int = None):
        with requests.Session() as s:
            headers= {"Authorization": f"Bearer {self.api_token}",
                      }
            payload= dict(offset=offset) if offset else None
            response = s.get(url=settings.INVASK_API_URL, headers=headers, params=payload)
            if not response.status_code == 200:
                logger.warning(msg=f"Во время загрузки данных с {settings.INVASK_API_URL} произошла ошибка\n"
                                   f"Ответ сервера: {response.status_code} \n {response.text}")
                s_exit()
            res_dict = response.json()
            return  res_dict.get("total"), res_dict.get("products")


    def get_stock(self) -> list:
        total, pr_list = self.table_requester()
        while total > len(pr_list):
            total, pr_batch_list = self.table_requester(offset=len(pr_list))
            pr_list += pr_batch_list

        return pr_list

    def register_download(self) -> list:
        with requests.Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}
            payload = dict(last_id=self.last_id) if self.last_id else dict()
            response = s.post(url=settings.OZON_STOCK_URL, headers=headers, json=payload)
            res_dict = response.json()
            print(res_dict)
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

    @staticmethod
    def process_table(product_list: list) -> pd.core.frame.DataFrame:
        string_list = list(
            filter(lambda x: isinstance(x.get("quantityLabel"), str) and x.get("quantityLabel").startswith(">"),
                   product_list))
        num_list = list(filter(lambda x: isinstance(x.get("quantityLabel"), int), product_list))

        conv_string_list = list(
            map(lambda x: {"cat_number": str(x.get("cat_number")), "quantityLabel": str(int(x.get("quantityLabel")[1:]) + 1)},
                string_list))
        conv_num_list = list(
            map(lambda x: {"cat_number": str(x.get("cat_number")), "quantityLabel": str(x.get("quantityLabel"))}, num_list))

        super_list = conv_string_list + conv_num_list
        df_invask = pd.DataFrame(super_list)
        df_invask = df_invask.replace(eval(settings.OZON_MIN_ITEMS), 0)
        # df_invask = df_invask.replace(">10", settings.OZON_MAX_ITEMS).replace(eval(settings.OZON_MIN_ITEMS), 0)
        df_invask.rename(columns={df_invask.columns[0]: 'offer_id', df_invask.columns[1]: 'stock_val'}, inplace=True)
        # df_invask = df_invask.iloc[:, :]
        return df_invask


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
    tg = TableGetter(api_token=settings.INVASK_API_TOKEN)
    product_list = tg.get_stock()
    df_invask = tg.process_table(product_list=product_list)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY)
    stock_list = oa.get_stock_items()
    batches2send, len_list = oa.process_stock_items(stock_list, df_site=df_invask)
    oa.update_stock(list_send=batches2send, len_list=len_list)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада выполнено за {delta}")


# if __name__ == '__main__':
    # exec_hour, exec_min = tuple(settings.TIME_EXECUTE.split(":"))
    #
    # # Preare our schedule hour list for sleeping until exec time
    # scheduler_list = [i for i in range(24)]
    # exec_val_index = scheduler_list.index(int(exec_hour))
    # del scheduler_list[exec_val_index]
    # del scheduler_list[exec_val_index-1]
    #
    # while True:
    #     time_pc = datetime.now()
    #     time_pc_hour, time_pc_min = time_pc.strftime("%H"), time_pc.strftime("%M")
    #
    #     if time_pc_hour == exec_hour:
    #         logger.info(msg=f"{time_pc} - запускаю обработчик")
    #         runner()
    #         logger.info(msg=f"Обработчик запустится через сутки - а пока баиньки")
    #         sleep(7200)
    #     if time_pc_hour in scheduler_list:
    #         logger.info(msg=f"{time_pc} время для запуска еще не настало")
    #         sleep(3600)
    #     else:
    #         logger.info(msg=f"{time_pc} скоро запустится обработчик")
    #         sleep(60)

# if __name__ == '__main__':
#     tg = TableGetter(api_token=settings.INVASK_API_TOKEN)
#     product_list = tg.get_stock()
#     string_list = list(filter(lambda x: isinstance(x.get("quantityLabel"), str) and x.get("quantityLabel").startswith(">"), product_list))
#     num_list = list(filter(lambda x: isinstance(x.get("quantityLabel"), int), product_list))
#
#     conv_string_list = list(map(lambda x: {"cat_number":str(x.get("cat_number")), "quantityLabel":str(int(x.get("quantityLabel")[1:])+1)}, string_list))
#     conv_num_list = list(map(lambda x: {"cat_number":str(x.get("cat_number")), "quantityLabel":str(x.get("quantityLabel"))}, num_list))
#
#     super_list = conv_string_list + conv_num_list
#
#     # print(*super_list, sep='\n')
#     df_invask = pd.DataFrame(super_list)
#     df_invask = df_invask.replace(eval(settings.OZON_MIN_ITEMS), 0)
#     df_invask.rename(columns={df_invask.columns[0]: 'offer_id', df_invask.columns[1]: 'stock_val'}, inplace=True)
#     # df_invask = df_invask.iloc[,:]
#     oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY)
#     stock_list = oa.get_stock_items()
#     # print(*stock_list, sep='\n')
#     batches2send, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_invask)
#     oa.update_stock(list_send=batches2send, len_list=len_list)

if __name__ == '__main__':
    while True:
        time_pc = datetime.now()
        logger.info(msg=f"{time_pc} - запускаю обработчик")
        runner()
        logger.info(msg=f"Обработчик запустится через час - а пока баиньки")
        sleep(3600)
