import sys
from pathlib import Path
from enum import Enum
from typing import Dict, List, Any
from queue import Empty
from threading import Thread
from multiprocessing import Process, Queue
from collections import defaultdict
from copy import deepcopy, copy
from datetime import datetime, time
from dateutil.relativedelta import relativedelta

from vnpy_datarecorder.engine import RecorderEngine, EVENT_RECORDER_EXCEPTION
from vnpy.trader.object import ContractData, TickData,BarData
from vnpy.trader.utility import get_file_path, BarGenerator
from vnpy.trader.database import BaseDatabase 
from vnpy.event import Event
from vnpy.trader.constant import Product
from longsheng_mysql_database import LongShengMysqlDatabase

def consumer(queue: Queue) -> None:
    """"""
    pre_print_info_dt = datetime.now()
    # 行情写数据库
    database: LongShengMysqlDatabase = LongShengMysqlDatabase()

    while True:
        try:
            task: Any = queue.get(timeout=1)
            task_type, data = task

            if task_type == "tick":
                database.save_tick_data(data, stream=True)
            if task_type == "contract":
                database.save_contract_data(data)

            # 每隔N分钟，还有多少个队列没写文件
            if datetime.now() > pre_print_info_dt + relativedelta(seconds=30):
                pre_print_info_dt = datetime.now()
                print(f"{datetime.now()},还有多少个队列没写文件：{queue.qsize()}")

        except Empty:
            continue

        except Exception:
            info = sys.exc_info()
            print(f"行情写文件进程出错：{info}")

class WholeMarketRecorder(RecorderEngine):
    """录制全量的tick行情"""

    data_filename: str = "vnpy_contract_data.db"
    data_filepath: str = str(get_file_path(data_filename))

    def __init__(self, main_engine, event_engine):
        
        # 不要系统提供的数据库操作类
        #self.database: LongShengMysqlDatabase = LongShengMysqlDatabase()

        self.all_ticks_list: List[TickData] = []
        self.all_contracts_list: List[ContractData] = []
        self.subcribed_contracts_list: List[ContractData] = []
        self.vnpy_contracts: Dict[str, ContractData] = {}
        self.timer_init_already = 60
        self.timer_init_couter = 0
        self.subcribed_one_time = 2000

        self.consumer_process_pool: List[Process] = []
        
        # 以下至方法结束，都是父类的属性及初始化方法，本来用suer()就可以解决的，但由于子类要自己的database，和父类的重复了，ORM又不能重复链接，所有把父类的init的内容拷贝过来
        #super().__init__(main_engine, event_engine)
        self.main_engine = main_engine
        self.event_engine = event_engine             
        
        self.queue: Queue = Queue()
        self.thread: Thread = Thread(target=self.run)
        self.active: bool = False

        self.tick_recordings: Dict[str, Dict] = {}
        self.bar_recordings: Dict[str, Dict] = {}
        self.bar_generators: Dict[str, BarGenerator] = {}

        self.timer_count: int = 0
        self.timer_interval: int = 0
        self.tick_count: int = 0

        self.ticks: Dict[str, List[TickData]] = defaultdict(list)
        self.bars: Dict[str, List[BarData]] = defaultdict(list)

        #self.database: BaseDatabase = get_database()

        self.load_setting()
        self.register_event()
        self.start()
        self.put_event()

    def start(self):
        for i in range(5):
            tmp_consumer = Process(target=consumer, args=(self.queue,))
            tmp_consumer.daemon = True # 父进程退出，子进程自动退出
            tmp_consumer.start()
            self.consumer_process_pool.append(tmp_consumer)           

    def run(self) -> None:
        pass

    def load_setting(self):
        """把父类的从json文件中，加载需要记录哪些合约，这个方法覆盖掉"""
        pass

    def save_setting(self):
        # 不更新原数据记录setting
        pass

    def process_contract_event(self, event):
        """当合约信息返回时，把所有的合约都添加进行情记录列表，请订阅行情"""
        contract:ContractData = event.data
        vt_symbol = contract.vt_symbol

        if self.vnpy_contracts.get(vt_symbol, None) is None:
            self.subcribed_contracts_list.append(contract)
            self.vnpy_contracts[vt_symbol] = contract
            self.all_contracts_list.append(contract)

        #不要直接订阅，等初始化完之后再订阅
        #self.subscribe(contract)       
    
    def process_timer_event(self, event: Event) -> None:
        """"""
        self.timer_count += 1
        if self.timer_count < self.timer_interval:
            return
        self.timer_count = 0

        if len(self.all_ticks_list) > 0:
            self.queue.put(("tick", copy(self.all_ticks_list)))
            self.all_ticks_list.clear()

        # 系统启动N秒(2*60)秒后，才开始订阅行情，让系统有足够的时间去处理初始化
        self.timer_init_couter += 1
        contract_size = len(self.subcribed_contracts_list)
        if self.timer_init_couter > self.timer_init_already and contract_size > 0:
            print(f"{datetime.now()},当前还有待订阅行情的合约数：{contract_size}")
            # 每1秒最大订阅N个合约，超过了就下一秒再订
            if contract_size > self.subcribed_one_time:
                contract_size = self.subcribed_one_time
                for i in range(contract_size): # 0 contract_size-1
                    if self.subcribed_contracts_list[i].product != Product.SPREAD: # 除了价差都订阅
                        self.subscribe(self.subcribed_contracts_list[i])
                
                self.subcribed_contracts_list = self.subcribed_contracts_list[contract_size+1:]
            else:
                for i in range(contract_size): # 0 contract_size-1
                    if self.subcribed_contracts_list[i].product != Product.SPREAD:
                        self.subscribe(self.subcribed_contracts_list[i])
                self.subcribed_contracts_list.clear()
        
        # 记录合约信息，只在11:40 - 12:00做一次就可以了
        if len(self.all_contracts_list) > 0 and datetime.now().time() > time(11,40) and datetime.now().time() < time(12,0):
            self.queue.put(("contract", deepcopy(self.all_contracts_list)))
            self.all_contracts_list.clear()
    
    def update_tick(self, tick: TickData) -> None:
        """"""
        self.all_ticks_list.append(copy(tick))
        #self.tick_count += 1
        #print(f"行情更新数量：{self.tick_count}")
