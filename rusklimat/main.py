# import requests
from requests import Session
import logging as logger
import pandas as pd
from datetime import datetime
from prices_reader import PriceReader
from sys import exit as s_exit
from time import sleep, time
from typing import Generator

from config import settings
logger.basicConfig(level=logger.INFO, format="%(asctime)s %(levelname)s %(message)s")


class TableGetter:

    @staticmethod
    def jwt_requester() -> str:
        with Session() as s:
            headers = {'User-Agent': 'catalog-ip'}
            request_body = {"login": settings.RUSKLIMAT_LOGIN ,
                            "password": settings.RUSKLIMAT_PASSWORD}
            response = s.post(url=settings.RUSKLIMAT_URL_JWT, headers=headers, json=request_body)
            if not response.status_code == 200:
                logger.warning(msg=f"Во время получения JWT с {settings.RUSKLIMAT_URL_JWT} произошла ошибка\n"
                                   f"Ответ сервера: {response.status_code} \n {response.text}")
                s_exit()

            rq_dict = response.json()
            if not rq_dict.get('code') == 200:
                logger.warning(msg=f"Во время получения JWT с {settings.RUSKLIMAT_URL_JWT} сервер не отдал токен\n"
                                   f"Ответ сервера: {rq_dict}")
                s_exit()

            return rq_dict['data']['jwtToken']

    @staticmethod
    def req_key_requester(s: Session, jwt: str) -> str:

        headers = {'Authorization': jwt}

        response = s.get(url=settings.RUSKLIMAT_URL_RQ, headers=headers)
        if not response.status_code == 200:
            logger.warning(msg=f"Во время получения REQUEST-KEY с {settings.RUSKLIMAT_URL_RQ} произошла ошибка\n"
                               f"Ответ сервера: {response.status_code} \n {response.text}")
            s_exit()

        rq_dict = response.json()
        if not rq_dict.get('requestKey'):
            logger.warning(msg=f"Во время получения  REQUEST-KEY с {settings.RUSKLIMAT_URL_RQ} сервер не отдал "
                               f"request-key\n"
                               f"Ответ сервера: {rq_dict}")
            s_exit()

        return rq_dict['requestKey']

    @staticmethod
    def rusclimat_get_data(s: Session, jwt: str, request_key: str, page: int=1):
        headers = {'Authorization': jwt}
        data_json = {
            "columns": [
                "nsCode",
                "vendorCode",
                "internetPrice",
                "remains"
            ],
            "filter": {
            },
            "sort": {
                "nsCode": "asc"
            }
        }
        page_params = f'/?pageSize=1000&page={page}'
        response = s.post(url=settings.RUSKLIMAT_URL_DATA + request_key + page_params, headers=headers, json=data_json)
        if not response.status_code == 200:
            logger.warning(msg=f"Во время загрузки данных с {response.url} произошла ошибка\n"
                               f"Ответ сервера: {response.status_code} \n {response.text}")
            s_exit()
        res_dict = response.json()

        if not res_dict.get('totalCount'):
            logger.warning(msg=f"Во время загрузки данных с {response.url} произошла ошибка\n"
                               f"Ответ сервера: {response.text}")
            s_exit()
        else:
            processed_res = list(map(lambda x: [x["nsCode"],
                                                x['remains']['warehouses'].get('фрц Киржач', 0) if x['remains']['total'] != 'ожидается поставка' \
                                                    else 0, str(int(x["internetPrice"]))] if x["internetPrice"] is not None else None,
                                     res_dict['data']))

            return processed_res, res_dict.get('totalPageCount')

    @staticmethod
    def data_requester(s: Session, jwt: str, request_key: str) -> list:

        res_list = []
        page = 1
        res_batch, total_pages = TableGetter.rusclimat_get_data(s=s, jwt=jwt, request_key=request_key)
        res_list.extend(res_batch)
        if total_pages > page:
            for i in range(page+1, total_pages+1):
                res_batch, total_pages = TableGetter.rusclimat_get_data(s=s, jwt=jwt, request_key=request_key, page=i)
                res_list.extend(res_batch)
            return res_list

    @staticmethod
    def table_requester(jwt: str) -> list:
        with Session() as s:
            request_key = TableGetter.req_key_requester(s=s, jwt=jwt)
            received_list = list(filter(lambda x: x is not None, TableGetter.data_requester(s=s, jwt=jwt,
                                                                                       request_key=request_key)))
            res_list = list(map(lambda x: [x[0], '500' if x[1] == 'более 500' else str(x[1]), x[2]], received_list))
            return res_list

    @staticmethod
    def process_table(product_list: list) -> pd.core.frame.DataFrame:

        # string_list = list(
        #     filter(lambda x: isinstance(x.get("quantityLabel"), str) and x.get("quantityLabel").startswith(">"),
        #            product_list))
        # num_list = list(filter(lambda x: isinstance(x.get("quantityLabel"), int), product_list))

        # conv_string_list = list(
        #     map(lambda x: {"cat_number": str(x.get("cat_number")),
        #                    "quantityLabel": str(int(x.get("quantityLabel")[1:]) + 1),
        #                    "regular_price": str(x.get("regular_price"))},
        #         string_list))
        # conv_num_list = list(
        #     map(lambda x: {"cat_number": str(x.get("cat_number")),
        #                    "quantityLabel": str(x.get("quantityLabel")),
        #                    "regular_price": str(x.get("regular_price"))
        #                    }, num_list))

        # super_list = conv_string_list + conv_num_list
        proc_product_list = list(map(lambda x: {"cat_number": x[0],
                           "quantityLabel": x[1],
                           "regular_price": x[2]}, product_list))
        df_rusklimat = pd.DataFrame(proc_product_list)
        df_rusklimat = df_rusklimat.replace(eval(settings.OZON_MIN_ITEMS), 0)
        df_rusklimat.rename(columns={df_rusklimat.columns[0]: 'offer_id', df_rusklimat.columns[1]: 'stock_val'}, inplace=True)
        # df_invask = df_invask.iloc[:, :]
        return df_rusklimat


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
            batch_list = self.list_batcher(list_dicts=stock_quants, n=1000)
            logger.info(msg=f"Завершено сопоставление таблицы артикулов и таблицы поставщика.\n"
                        f"Обработано {len(stock_quants)} записей для цен")
            return batch_list, len(stock_quants)

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
                    logger.info(msg=f"Пачка данных цен обработана: {response.json()}")
                if len_list > 8000:
                    sleep(1)

    @staticmethod
    def list_batcher(list_dicts: list, n: int = 100):
        len_list = len(list_dicts)
        for ndx in range(0, len_list, n):
            yield list_dicts[ndx:min(ndx + n, len_list)]


def runner(prices_dd: dict):
    start = time()

    jwt = TableGetter.jwt_requester()
    table_list = TableGetter.table_requester(jwt=jwt)
    df_rusklimat = TableGetter.process_table(product_list=table_list)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict=prices_dd)
    stock_list = oa.get_stock_items()

    batches2send, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_rusklimat)
    batches2send_p, len_list_p = oa.process_stock_items(stock_list=stock_list, df_site=df_rusklimat, price_flag=True)

    oa.update_stock(list_send=batches2send, len_list=len_list)
    oa.update_stock(list_send=batches2send_p, len_list=len_list, price_flag=True)


    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада и {len_list_p} по ценам выполнено за {delta}")


def runner_stock():
    start = time()

    jwt = TableGetter.jwt_requester()
    table_list = TableGetter.table_requester(jwt=jwt)
    df_rusklimat = TableGetter.process_table(product_list=table_list)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict={ })
    stock_list = oa.get_stock_items()

    batches2send, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_rusklimat)

    oa.update_stock(list_send=batches2send, len_list=len_list)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада выполнено за {delta}")


def runner_price(prices_dd: dict):
    start = time()

    jwt = TableGetter.jwt_requester()
    table_list = TableGetter.table_requester(jwt=jwt)
    df_rusklimat = TableGetter.process_table(product_list=table_list)

    oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict=prices_dd)
    stock_list = oa.get_stock_items()

    batches2send_p, len_list = oa.process_stock_items(stock_list=stock_list, df_site=df_rusklimat, price_flag=True)

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
                    logger.info(msg=f"Обработчик запустится через {settings.UPDATE_PERIOD} сек - а пока баиньки")
                    sleep(settings.UPDATE_PERIOD)

            case '2':
                # pr = PriceReader()
                # prices_dd = pr.get_prices_dict()

                while True:
                    time_pc = datetime.now()
                    logger.info(msg=f"{time_pc} - запускаю обработчик")
                    runner_stock()
                    logger.info(msg=f"Обработчик запустится через {settings.UPDATE_PERIOD} сек - а пока баиньки")
                    sleep(settings.UPDATE_PERIOD)
            case '3':
                pr = PriceReader()
                prices_dd = pr.get_prices_dict()
                while True:
                    time_pc = datetime.now()
                    logger.info(msg=f"{time_pc} - запускаю обработчик")
                    runner_price(prices_dd=prices_dd)
                    logger.info(msg=f"Обработчик запустится через {settings.UPDATE_PERIOD} сек - а пока баиньки")
                    sleep(settings.UPDATE_PERIOD)

            case '4':
                s_exit()


if __name__ == '__main__':
    main_proc()

    # oa = OzonApi(client_id=settings.OZON_CLIENT_ID, api_key=settings.OZON_API_KEY, prices_delta_dict={})
    # stock_list = oa.get_stock_items()
    # print(list(map(lambda x: x.get('offer_id'), stock_list)), len(stock_list))

    # jwt = TableGetter.jwt_requester()
    # print(jwt)
    # table_list =TableGetter.table_requester(jwt=jwt)
    # print(table_list)
    # search_pos= list(filter(lambda x: x[1] != '0', table_list))
    # df = TableGetter.process_table(product_list=table_list)
    # print(df)
    # pr = PriceReader()
    # prices_dd = pr.get_prices_dict()


