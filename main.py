import requests
import logging as logger
import pandas as pd
from datetime import datetime
from io import BytesIO
from prices_reader import PriceReader
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
            headers = {"Authorization": f"Bearer {self.api_token}",
                      }
            payload = dict(offset=offset) if offset else None
            response = s.get(url=settings.INVASK_API_URL, headers=headers, params=payload)
            if not response.status_code == 200:
                logger.warning(msg=f"Во время загрузки данных с {settings.INVASK_API_URL} произошла ошибка\n"
                                   f"Ответ сервера: {response.status_code} \n {response.text}")
                s_exit()
            res_dict = response.json()
            return res_dict.get("total"), res_dict.get("products")

    def get_stock(self) -> list:
        total, pr_list = self.table_requester()
        while total > len(pr_list):
            total, pr_batch_list = self.table_requester(offset=len(pr_list))
            pr_list += pr_batch_list

        return pr_list

    @staticmethod
    def get_name() -> str:
        time_str = datetime.now().strftime("%Y_%m_%d %H_%M_%S")
        return f"{time_str} {settings.TABLE_NAME}"

    @staticmethod
    def save_excel(product_list: list):
        for el in product_list:
            if el.get("attributes"):
                for k, v in el.get("attributes").items():
                    el[k] = v
                del el["attributes"]
        df = pd.DataFrame(product_list)
        filename = TableGetter.get_name()
        df.to_excel(filename)

        writer = pd.ExcelWriter(filename, engine="xlsxwriter")
        df.to_excel(writer, sheet_name=settings.SHEET_NAME, index=False, na_rep='NaN')

        # Auto-adjust columns' width
        for column in df:
            column_width = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets[settings.SHEET_NAME].set_column(col_idx, col_idx, column_width)

        writer.close()

    def register_download(self) -> list:
        with requests.Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}
            payload = dict(last_id=self.last_id) if self.last_id else dict()
            response = s.post(url=settings.OZON_STOCK_URL, headers=headers, json=payload)
            res_dict = response.json()
            last_id = res_dict.get("result").get("last_id")
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
            map(lambda x: {"cat_number": str(x.get("cat_number")),
                           "quantityLabel": str(int(x.get("quantityLabel")[1:]) + 1),
                           "regular_price": str(x.get("regular_price"))},
                string_list))
        conv_num_list = list(
            map(lambda x: {"cat_number": str(x.get("cat_number")),
                           "quantityLabel": str(x.get("quantityLabel")),
                           "regular_price": str(x.get("regular_price"))
                           }, num_list))

        super_list = conv_string_list + conv_num_list
        df_invask = pd.DataFrame(super_list)
        df_invask = df_invask.replace(eval(settings.OZON_MIN_ITEMS), 0)
        # df_invask = df_invask.replace(">10", settings.OZON_MAX_ITEMS).replace(eval(settings.OZON_MIN_ITEMS), 0)
        df_invask.rename(columns={df_invask.columns[0]: 'offer_id', df_invask.columns[1]: 'stock_val'}, inplace=True)
        # df_invask = df_invask.iloc[:, :]
        return df_invask


class SimaLandApi:
    def __init__(self, phone: str, password: str):
        self.jwt = SimaLandApi.get_jwt(phone=phone, password=password)

    @staticmethod
    def get_jwt(phone: str, password: str) -> str:
        print(phone, password)
        with requests.Session() as s:
            # headers = {'Client-Id': self.client_id,
            #            'Api-Key': self.api_key}
            payload = dict(phone=phone, password=password, regulation=True)

            response = s.post(url=f"{settings.SIMA_LAND_URL}signin", json=payload)
            print(response.status_code, response.text)
        return ' '

    def get_stock_items_batch(self):
        with requests.Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}
            payload = dict(last_id=self.last_id) if self.last_id else dict()
            response = s.post(url=f"{settings.SIMA_ISLAND_URL}items", headers=headers, json=payload)


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
        with requests.Session() as s:
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

    def process_stock_items(self, stock_list: list, df_site: pd.core.frame.DataFrame, price_flag: bool = False) -> tuple:
        df_stock_raw = pd.DataFrame(stock_list)
        df_stock = df_stock_raw[['product_id', 'offer_id']]
        df_stock_quants = pd.DataFrame(columns=['offer_id', 'product_id', 'stock', 'warehouse_id'])

        stock_quants = []
        for index, row in df_stock[["product_id", "offer_id"]].iterrows():

            key_oid = row["offer_id"]
            key_pid = row["product_id"]
            # get stock value from supplier table
            if not price_flag:
                stock_value = tuple(df_site[df_site['offer_id'] == key_oid]['stock_val'])

                if len(stock_value) == 0:
                    continue
                stock_quants.append(dict(offer_id=key_oid,
                                         product_id=key_pid,
                                         stock=stock_value[0],
                                         warehouse_id=settings.OZON_WAREHOUSE_ID))
            else:
                price_value = tuple(df_site[df_site['offer_id'] == key_oid]['regular_price'])

                price_delta = self.prices_dd.get(key_oid)
                if len(price_value) == 0 or price_delta is None:
                    # print(price_value, price_delta)
                    continue
                # if key_oid == '451873':
                #     print(price_value, price_delta)
                updated_value = PriceReader.price_process(price=int(price_value[0]), price_delta=price_delta)
                stock_quants.append(dict(offer_id=key_oid,
                                         product_id=key_pid,
                                         old_price="0",
                                         price=updated_value,
                                         # price=price_value[0],
                                         # updated_price=updated_value
                                         ))
        if not price_flag:
            df_stock_quants = pd.concat([df_stock_quants, pd.DataFrame(stock_quants)])
            result_list_dicts = df_stock_quants.to_dict(orient="records")
            batch_list = self.list_batcher(list_dicts=result_list_dicts)
            logger.info(msg=f"Завершено сопоставление таблицы артикулов и таблицы поставщика.\n"
                        f"Обработано {len(result_list_dicts)} записей для кол-ва товаров")
            return batch_list, len(result_list_dicts)
        else:
            # print(stock_quants[:10])
            batch_list = self.list_batcher(list_dicts=stock_quants[:10], n=1000)
            logger.info(msg=f"Завершено сопоставление таблицы артикулов и таблицы поставщика.\n"
                        f"Обработано {len(stock_quants)} записей для цен")
            return batch_list, len(stock_quants)

    def update_stock(self, list_send: Generator, len_list: int, price_flag: bool = False):
        with requests.Session() as s:
            headers = {'Client-Id': self.client_id,
                       'Api-Key': self.api_key}

            for el in list_send:
                if not price_flag:
                    payload = dict(stocks=el)
                    response = s.post(url=settings.OZON_STOCK_UPDATE_URL, headers=headers, json=payload)
                    logger.info(msg=f"Пачка данных кол-ва товаров обработана:{response.json()}")
                else:
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


def runner(prices_dd: dict):
    start = time()

    tg = TableGetter(api_token=settings.INVASK_API_TOKEN)
    product_list = tg.get_stock()

    df_invask = tg.process_table(product_list=product_list)
    # print(df_invask)
    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict=prices_dd)
    stock_list = oa.get_stock_items()

    batches2send, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_invask)
    batches2send_p, len_list_p = oa.process_stock_items(stock_list=stock_list, df_site=df_invask, price_flag=True)

    oa.update_stock(list_send=batches2send, len_list=len_list)
    oa.update_stock(list_send=batches2send_p, len_list=len_list, price_flag=True)


    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада и {len_list_p} по ценам выполнено за {delta}")


def runner_stock():
    start = time()

    tg = TableGetter(api_token=settings.INVASK_API_TOKEN)
    product_list = tg.get_stock()

    df_invask = tg.process_table(product_list=product_list)
    # print(df_invask)
    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict={ })
    stock_list = oa.get_stock_items()

    batches2send, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_invask)

    oa.update_stock(list_send=batches2send, len_list=len_list)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада выполнено за {delta}")


def runner_price(prices_dd: dict):
    start = time()

    tg = TableGetter(api_token=settings.INVASK_API_TOKEN)
    product_list = tg.get_stock()

    df_invask = tg.process_table(product_list=product_list)
    # print(df_invask)
    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict=prices_dd)
    stock_list = oa.get_stock_items()

    batches2send_p, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_invask, price_flag=True)

    oa.update_stock(list_send=batches2send_p, len_list=len_list, price_flag=True)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций ценам выполнено за {delta}")



def main_proc():
    option = input("Привет! Напишите вариант работы программы и нажмите enter\n\n"
                            "1. Скрипт обновит остатки склада и цены\n"
                            "2. Скрипт обновит только остатки склада\n"
                            "3. Скрипт обновит только цены\n"
                            "4. Выход\n\n\n"
                            "Ваш выбор: ").strip()

    if option not in ['1', '2', '3', '4']:
        print("напишите 1 или 2 или 3 или 4 и нажмите enter")
        main_proc()
    else:
        logger.info(msg="Выполняю...")
        match option:
            case '1':
                pr = PriceReader()
                prices_dd = pr.get_prices_dict()
                while True:
                    time_pc = datetime.now()
                    logger.info(msg=f"{time_pc} - запускаю обработчик")
                    runner(prices_dd=prices_dd)
                    logger.info(msg=f"Обработчик запустится через час - а пока баиньки")
                    sleep(3600)

                runner(prices_dd=prices_dd)
            case '2':
                # pr = PriceReader()
                # prices_dd = pr.get_prices_dict()

                while True:
                    time_pc = datetime.now()
                    logger.info(msg=f"{time_pc} - запускаю обработчик")
                    runner_stock()
                    logger.info(msg=f"Обработчик запустится через час - а пока баиньки")
                    sleep(3600)
            case '3':
                pr = PriceReader()
                prices_dd = pr.get_prices_dict()
                while True:
                    time_pc = datetime.now()
                    logger.info(msg=f"{time_pc} - запускаю обработчик")
                    runner_price(prices_dd=prices_dd)
                    logger.info(msg=f"Обработчик запустится через час - а пока баиньки")
                    sleep(3600)

            case '4':
                s_exit()



if __name__ == '__main__':
    main_proc()
    # while True:
    #     time_pc = datetime.now()
    #     logger.info(msg=f"{time_pc} - запускаю обработчик")
    #     runner(prices_dict=prices_dict)
    #     logger.info(msg=f"Обработчик запустится через час - а пока баиньки")
    #     sleep(3600)




# if __name__ == '__main__':
    # sl = SimaLandApi(phone=settings.SIMA_PHONE, password=settings.SIMA_PASS)


# if __name__ == '__main__':
#     tg = TableGetter(api_token=settings.INVASK_API_TOKEN)
#     product_list = tg.get_stock()
#     tg.save_excel(product_list=product_list)
    # pr_product_list = list(map(lambda x: x.update))

    # print(product_list[0].get("attributes"), type(product_list[0].get("attributes")))
    # string_list = list(filter(lambda x: isinstance(x.get("quantityLabel"), str) and x.get("quantityLabel").startswith(">"), product_list))
    # num_list = list(filter(lambda x: isinstance(x.get("quantityLabel"), int), product_list))

    # conv_string_list = list(map(lambda x: {"cat_number":str(x.get("cat_number")), "quantityLabel":str(int(x.get("quantityLabel")[1:])+1)}, string_list))
    # conv_num_list = list(map(lambda x: {"cat_number":str(x.get("cat_number")), "quantityLabel":str(x.get("quantityLabel"))}, num_list))

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