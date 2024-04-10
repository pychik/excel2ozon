import requests
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
    def table_from_excel() -> pd.core.frame.DataFrame:
        try:
            df_supplies = pd.read_excel(settings.EXCEL_FILE, sheet_name=settings.EXCEL_SHEET_NAME, usecols=f'{settings.EXCEL_OFFER_ID_COL}, {settings.EXCEL_QUANTITY_COL}')
            df_supplies.drop(df_supplies.index[0])
            headers = ['sku', "amount", ]
            df_supplies.columns = headers
            return df_supplies
        except Exception as e:
            logger.warning(msg=f"Во время загрузки таблицы произошла ошибка {e}")
            sleep(5)
            s_exit()


class WB:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.last_id = None
        self.total = 1000
        self.updated_at = ''
        self.res_list = []
        self.sku_list = []

    def get_stock_items(self):
        while self.total >= 1000:
            self.res_list += self.get_stock_items_batch()
        logger.info(msg="Данные с вайлдберис склада получены")
        return self
        # return self.res_list

    def get_skus(self):
        for el in self.res_list:
            for s in el.get('sizes'):
                self.sku_list.append(s.get('skus')[0])
        return self

    def get_stock_items_batch(self):
        with requests.Session() as s:
            headers = {'accept': 'application/json',
                       'Authorization': self.api_key,
                       'Content-Type': 'application/json',
                       }
            # payload = dict(last_id=self.last_id) if self.last_id else dict()
            from json import dumps
            payload = {"settings": {
                            "cursor": {
                              "limit": 1000,
                              "updatedAt": self.updated_at,
                              "nmID": self.last_id
                            },
                            "filter": {
                              "withPhoto": -1,
                            }
                      }
                    } if self.last_id else {"settings": {
                            "cursor": {
                              "limit": 1000
                            },
                            "filter": {
                              "withPhoto": -1,
                            }
                      }
                    }

            response = s.post(url=settings.WB_STOCK_URL, headers=headers, json=payload)

            res_dict = response.json()

            result = res_dict

            if not result:
                logger.warning(msg=f"Во время выгрузки данных товаров на вайлдберис произошла ошибка ")
                sleep(5)
                s_exit()
            else:
                self.last_id = result.get("cursor",).get('nmID')
                self.updated_at = result.get("cursor",).get('nmID')
                self.total = result.get("cursor",).get('total')

                batch_list = res_dict.get('cards')
                if not batch_list:
                    logger.warning(msg=f"Во время выгрузки данных товаров на вайлдберис произошла ошибка {res_dict}")
                    sleep(5)
                    s_exit()

                return batch_list

    def process_stock_items(self, stock_list: list, df_site: pd.core.frame.DataFrame) -> tuple:
        df_stock = pd.DataFrame(stock_list, columns=['sku'])

        df_stock_quants = pd.DataFrame(columns=['sku', 'amount'])

        stock_quants = []
        for index, row in df_stock[["sku"]].iterrows():

            key_sku = row["sku"]
            stock_value = tuple(df_site[df_site['sku'] == int(key_sku)]['amount'])

            if len(stock_value) == 0:
                continue
            stock_quants.append(dict(sku=key_sku,
                                     amount=stock_value[0],
                                     ))

        df_stock_quants = pd.concat([df_stock_quants, pd.DataFrame(stock_quants)])
        result_list_dicts = df_stock_quants.to_dict(orient="records")

        batch_list = self.list_batcher(list_dicts=result_list_dicts)
        logger.info(msg=f"Завершено сопоставление таблицы артикулов и таблицы поставщика.\n"
                        f"Обработано {len(result_list_dicts)} записей для кол-ва товаров")
        return batch_list, len(result_list_dicts)

    @staticmethod
    def list_batcher(list_dicts: list, n: int = 100):
        len_list = len(list_dicts)
        for ndx in range(0, len_list, n):
            yield list_dicts[ndx:min(ndx + n, len_list)]

    def update_stock(self, list_send: Generator, len_list: int, price_flag: bool = False):
        with requests.Session() as s:
            headers = {'accept': 'application/json',
                       'Authorization': self.api_key,
                       'Content-Type': 'application/json',
                       }
            url = f"{settings.WB_STOCK_UPDATE_URL}/{settings.WB_WAREHOUSE_ID}"
            for el in list_send:

                payload = dict(stocks=el)

                response = s.put(url=url, headers=headers,
                                 json=payload)
                logger.info(msg=f"Пачка данных кол-ва товаров обработана,\nОтвет сервера:{response.status_code}\n"
                                f"Сообщение от сервера: {response.text}")


def runner_stock():
    start = time()

    wb = WB(api_key=settings.WB_API_KEY)

    df_wb = TableGetter.table_from_excel()
    stock_list = wb.get_stock_items().get_skus().sku_list

    batches2send, len_list = wb.process_stock_items(stock_list=stock_list, df_site=df_wb)
    wb.update_stock(list_send=batches2send, len_list=len_list)

    finish = time()
    delta = finish - start
    logger.info(msg=f"Обновление {len_list} позиций по остаткам склада выполнено за {delta}")


def main_proc():
    while True:
        time_pc = datetime.now()

        start_time = datetime.strptime(settings.START_TIME, '%H:%M').replace(year=time_pc.year)\
            .replace(month=time_pc.month).replace(day=time_pc.day)

        stop_time = datetime.strptime(settings.STOP_TIME, '%H:%M').replace(year=time_pc.year)\
            .replace(month=time_pc.month).replace(day=time_pc.day)

        if start_time < time_pc < stop_time:
            logger.info(msg=f"{time_pc} - запускаю обработчик")
            runner_stock()
            logger.info(msg=f"Обработчик запустится через {settings.UPDATE_PERIOD} сек - а пока баиньки")
            sleep(settings.UPDATE_PERIOD)
        else:
            sleep(5)


if __name__ == '__main__':
    main_proc()
