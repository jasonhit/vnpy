import traceback
import importlib
import os,sys
from types import ModuleType
from typing import List, Dict, Set, Callable, Any, Optional
from collections import defaultdict
from copy import copy
from pathlib import Path
from datetime import datetime, timedelta
from queue import Empty
from multiprocessing import Process, Queue
from dateutil.relativedelta import relativedelta

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TICK, EVENT_POSITION, EVENT_CONTRACT,
    EVENT_ORDER, EVENT_TRADE, EVENT_TIMER
)
from vnpy.trader.utility import load_json, save_json
from vnpy.trader.object import (
    TickData, ContractData, BarData,
    PositionData, OrderData, TradeData, LogData,
    SubscribeRequest, OrderRequest, CancelRequest
)
from vnpy.trader.constant import (
    Direction, Offset, OrderType, Interval
)
from vnpy_spreadtrading.base import (
    LegData, SpreadData,
    EVENT_SPREAD_DATA, EVENT_SPREAD_POS,
    EVENT_SPREAD_ALGO, EVENT_SPREAD_LOG,
    EVENT_SPREAD_STRATEGY,
    load_bar_data, load_tick_data
)
from ls_savespreaddata.market_data_csv_store import MarketDadaCsvStore


APP_NAME = "SaveSpreadData"

def consumer(queue: Queue) -> None:
    """"""
    pre_print_info_dt = datetime.now()
    # 行情写csv文件
    market_data_csv_store = MarketDadaCsvStore()

    while True:
        try:
            task: Any = queue.get(timeout=1)
            task_type, data = task

            if task_type == "spread":
                market_data_csv_store.write2csv(data)

        except Empty:
            continue

        except Exception:
            info = sys.exc_info()
            print(f"行情写文件进程出错：{info}")

class SaveSpreadDataEngine(BaseEngine):
    """"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """Constructor"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.active: bool = False

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.write_log = main_engine.write_log

        self.all_spread_data_list: List[Any] = [] # 每个list的元素是1个元组(组合名称，string类型的head title，string类型价差数据)
        self.spread_data_map: Dict[str, Any] = {} # key是组合名称，value是(spread_bid,spread_ask)元组对象

        self.timer_count: int = 0
        self.timer_interval: int = 20

        self.queue: Queue = Queue()
        self.process: Process = Process(target=consumer, args=(self.queue,))
        self.start_process()

    def start(self) -> None:
        """"""
        if self.active:
            return
        self.active = True

        self.register_event()

        self.write_log("价差记录引擎启动成功")

    def stop(self) -> None:
        """"""
        pass

    def write_log(self, msg: str) -> None:
        """"""
        log: LegData = LogData(
            msg=msg,
            gateway_name=APP_NAME
        )
        event: Event = Event(EVENT_SPREAD_LOG, log)
        self.event_engine.put(event)
    
    def start_process(self):
        self.process.daemon = True # 父进程退出，子进程自动退出
        self.process.start()

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(
            EVENT_SPREAD_DATA, self.process_spread_event
        )
    
    def process_timer_event(self, event: Event) -> None:
        self.timer_count += 1
        if self.timer_count > self.timer_interval:
            self.timer_count = 0

            if len(self.all_spread_data_list) > 0:
                self.queue.put(("spread", copy(self.all_spread_data_list)))
                self.all_spread_data_list.clear()

    def process_spread_event(self, event: Event) -> None:
        """"""
        spread: SpreadData = event.data
        print(f"组合名称:{spread.name},Bid_Price:{spread.bid_price}, Ask_Price:{spread.ask_price}, Bid_Volume:{spread.bid_volume}, Ask_Volume:{spread.ask_volume}")

        #如果datetime 为None，则不需要记录，直接返回
        if spread.datetime is None:
            return
        
        #如果价差没有变化，则不需要记录，直接返回        
        if spread.name in self.spread_data_map:
            old_bid_price, old_ask_price = self.spread_data_map[spread.name]
            if old_bid_price == spread.bid_price and old_ask_price == spread.ask_price:
                return
        
        self.spread_data_map[spread.name] = (spread.bid_price, spread.ask_price)

        # 价差数据list
        tmp_spread_data_list = []
        tmp_spread_data_list.append(spread.datetime.strftime("%Y-%m-%d %H:%M:%S.%f"))
        tmp_spread_data_list.append('{:g}'.format(spread.bid_price))
        tmp_spread_data_list.append('{:g}'.format(spread.ask_price))
        tmp_spread_data_list.append('{:g}'.format(spread.bid_volume))
        tmp_spread_data_list.append('{:g}'.format(spread.ask_volume))

        for leg in spread.legs.values():
            tmp_spread_data_list.append(leg.bid_price)
            tmp_spread_data_list.append(leg.ask_price)
            tmp_spread_data_list.append(leg.bid_volume)
            tmp_spread_data_list.append(leg.ask_volume)
        
        compose_quote_data = ",".join([str(x) for x in tmp_spread_data_list])

        # 价差head title
        tmp_spread_head_list = []
        tmp_spread_head_list.append("时间")
        tmp_spread_head_list.append("价差买价")
        tmp_spread_head_list.append("价差卖价")
        tmp_spread_head_list.append("价差买量")
        tmp_spread_head_list.append("价差卖量")

        for leg in spread.legs.values():
            tmp_spread_head_list.append(f"{leg.vt_symbol}买价")
            tmp_spread_head_list.append(f"{leg.vt_symbol}卖价")
            tmp_spread_head_list.append(f"{leg.vt_symbol}买量")
            tmp_spread_head_list.append(f"{leg.vt_symbol}卖量")
        
        compose_quote_head = ",".join([str(x) for x in tmp_spread_head_list])

        self.all_spread_data_list.append((spread.name, compose_quote_head, compose_quote_data))



