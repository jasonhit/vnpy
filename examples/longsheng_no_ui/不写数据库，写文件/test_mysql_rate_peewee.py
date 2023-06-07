from datetime import datetime
from typing import Any,List
from threading import Thread
from queue import Queue, Empty
import random
from copy import deepcopy, copy
import sys

from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    DoubleField,
    IntegerField,
    Model,
    MySQLDatabase as PeeweeMySQLDatabase,
    ModelSelect,
    ModelDelete,
    chunked,
    fn
)
from playhouse.shortcuts import ReconnectMixin

from vnpy.trader.setting import SETTINGS
from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange

class ReconnectMySQLDatabase(ReconnectMixin, PeeweeMySQLDatabase):
    """带有重连混入的MySQL数据库类"""
    pass

db = ReconnectMySQLDatabase(
    database=SETTINGS["database.database"],
    user=SETTINGS["database.user"],
    password=SETTINGS["database.password"],
    host=SETTINGS["database.host"],
    port=SETTINGS["database.port"]
)

class DateTimeMillisecondField(DateTimeField):
    """支持毫秒的日期时间戳字段"""

    def get_modifiers(self):
        """毫秒支持"""
        return [3]

class DbTickData(Model):
    """TICK数据表映射对象"""

    id: AutoField = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    datetime: datetime = DateTimeMillisecondField()

    name: str = CharField()
    volume: float = DoubleField()
    turnover: float = DoubleField()
    open_interest: float = DoubleField()
    last_price: float = DoubleField()
    last_volume: float = DoubleField()
    limit_up: float = DoubleField()
    limit_down: float = DoubleField()

    open_price: float = DoubleField()
    high_price: float = DoubleField()
    low_price: float = DoubleField()
    pre_close: float = DoubleField()

    bid_price_1: float = DoubleField()
    bid_price_2: float = DoubleField(null=True)
    bid_price_3: float = DoubleField(null=True)
    bid_price_4: float = DoubleField(null=True)
    bid_price_5: float = DoubleField(null=True)

    ask_price_1: float = DoubleField()
    ask_price_2: float = DoubleField(null=True)
    ask_price_3: float = DoubleField(null=True)
    ask_price_4: float = DoubleField(null=True)
    ask_price_5: float = DoubleField(null=True)

    bid_volume_1: float = DoubleField()
    bid_volume_2: float = DoubleField(null=True)
    bid_volume_3: float = DoubleField(null=True)
    bid_volume_4: float = DoubleField(null=True)
    bid_volume_5: float = DoubleField(null=True)

    ask_volume_1: float = DoubleField()
    ask_volume_2: float = DoubleField(null=True)
    ask_volume_3: float = DoubleField(null=True)
    ask_volume_4: float = DoubleField(null=True)
    ask_volume_5: float = DoubleField(null=True)

    localtime: datetime = DateTimeMillisecondField(null=True)

    class Meta:
        database: PeeweeMySQLDatabase = db
        #indexes: tuple = ((("symbol", "exchange", "datetime"), False),)

    
class TestMysqlRatePeewee():
    """Mysql数据库接口"""

    def __init__(self) -> None:
        """"""
        self.db: PeeweeMySQLDatabase = db
        self.db.connect()
        self.db.create_tables([DbTickData])

        self.queue: Queue = Queue()
    
        self.producer_thread = Thread(target=self.producer)
        self.consumer_thread_pool: List[Thread] = []

        self.producer_thread.start()
        self.consumer_pool_start()
    
    def consumer_pool_start(self):
        for i in range(2):
            tmp_consumer = Thread(target=self.consumer)
            tmp_consumer.start()
            self.consumer_thread_pool.append(tmp_consumer)

    def consumer(self):
        """生产者"""
        while True:
            try:
                task: Any = self.queue.get(timeout=1)
                task_type, data = task

                if task_type == "tick":
                    self.save_tick_data(data, stream=True)

            except Empty:
                continue

            except Exception:
                info = sys.exc_info()
                print("消费出错，已暂停消费")

    def producer(self):
        """消费者"""
        while True:
            try:
                count = random.randint(10,15)
                ticks: List[TickData] = []
                for i in range(count):
                    tick: TickData = TickData(
                        symbol = "au2306",
                        exchange = Exchange.SHFE,
                        datetime = datetime.now(),
                        
                        name = "沪金2306",
                        volume = random.random(),
                        turnover = random.random(),
                        open_interest = random.random(),
                        last_price = random.random(),
                        last_volume = random.random(),
                        limit_up = random.random(),
                        limit_down = random.random(),

                        open_price = random.random(),
                        high_price = random.random(),
                        low_price = random.random(),
                        pre_close = random.random(),
                        bid_price_1 = random.random(),
                        ask_price_1 = random.random(),
                        bid_volume_1 = random.random(),
                        ask_volume_1 = random.random(),
                        gateway_name = "CTP"
                    )
                    ticks.append(tick)
                if len(ticks) > 0:
                    self.queue.put(("tick", deepcopy(ticks)))
                    ticks.clear()

            except Empty:
                continue

            except Exception:
                info = sys.exc_info()
                print(f"生产者出错，已暂停生产:{info}")

    
    def save_tick_data(self, ticks: List[TickData], stream: bool = False) -> bool:
        # 将TickData数据转换为字典
        data: list = []

        for tick in ticks:
            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        begin_time = datetime.now()
        # 使用upsert操作将数据更新到数据库中
        with self.db.atomic():
            for c in chunked(data, 200):
                #DbTickData.insert_many(c).on_conflict_replace().execute()
                DbTickData.insert_many(c).execute()
        
        print(f"{datetime.now()},行情当前执行插入数据库的数量：{len(ticks)}，耗时：{datetime.now()-begin_time}")


if __name__ == '__main__':
    test = TestMysqlRatePeewee()