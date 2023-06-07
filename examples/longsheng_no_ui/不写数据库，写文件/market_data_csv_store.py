import csv
from typing import List,Dict
import os
import sys
from datetime import datetime,time
from pathlib import Path
from io import TextIOWrapper
from dateutil.relativedelta import relativedelta
from collections import Counter

from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange

class MarketDadaCsvStore:
    def __init__(self):
        self.csvfiles: Dict[str, TextIOWrapper] = {}
        self.submap: Dict[str, csv.DictWriter] = {}
        self.day_str = datetime.now().strftime("%Y%m%d")
        self.dir_path = os.path.split(os.path.realpath(__file__))[0] + "\\" + self.day_str

        self.already_write_row = 0

        self.file_index = 1
        self.max_row_one_file = 500000
        self.row_count = 0
        self.pre_print_info_dt = datetime.now()

        self.csvfile: TextIOWrapper  = None
        #self.csvfile_w: csv.DictWriter = None

        self.csvheader=[
            "symbol",
            "exchange",
            "datetime",

            "name",
            "volume",
            "turnover",
            "open_interest",
            "last_price",
            "last_volume",
            "limit_up",
            "limit_down",

            "open_price",
            "high_price",
            "low_price",
            "pre_close",

            "bid_price_1",
            "bid_price_2",
            "bid_price_3",
            "bid_price_4",
            "bid_price_5",

            "ask_price_1",
            "ask_price_2",
            "ask_price_3",
            "ask_price_4",
            "ask_price_5",

            "bid_volume_1",
            "bid_volume_2",
            "bid_volume_3",
            "bid_volume_4",
            "bid_volume_5",

            "ask_volume_1",
            "ask_volume_2",
            "ask_volume_3",
            "ask_volume_4",
            "ask_volume_5"
        ]
    
        self.init_file_index()
        self.init_writer()

    def init_file_index(self):
        
        Path(self.dir_path).mkdir(parents=True, exist_ok=True)

        #如果该日期下的文件数量，新的文件数量的编号，递增1
        has_csv_count = len([f for f in os.listdir(self.dir_path) if os.path.isfile(os.path.join(self.dir_path, f))])
        #csv_files = Path.glob(self.dir_path, ".csv")
        if has_csv_count > 0:
            self.file_index += has_csv_count
        

    def init_writer(self):               
        csvname = f"{self.day_str}_{self.file_index}.csv"
        file_path = self.dir_path + "\\"  + csvname
        isExist = os.path.isfile(file_path)
        self.csvfile = open(file_path, 'a+', newline='')
        
        #self.csvfile_w = csv.DictWriter(self.csvfile, fieldnames=self.csvheader)
        self.csvfile_w = csv.writer(self.csvfile)
        if not isExist:
            #self.csvfile_w.writeheader()
            self.csvfile_w.writerow(self.csvheader)
            self.csvfile.flush()
            

    def write2csv(self, ticks: List[TickData]):
        """不同的合约行情，记在不同的文件中"""
        # 读取主键参数
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange
        key = symbol + "." + exchange.value

        data: list = []

        for tick in ticks:
            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("localtime")
            data.append(d)

            self.already_write_row += 1

        # 文件是否存在，并打开，如果没有，就打开文件
        if self.submap.get(key, None) is None:
            csvname = f"{key}.csv"
            
            dir_path = os.path.split(os.path.realpath(__file__))[0] + "\\" + self.day_str        
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            #print(f"文件目录：{dir_path}")
            
            file_path = dir_path + "\\"  + csvname
            isExist = os.path.isfile(file_path)
            csvfile = open(file_path, 'a+', newline='')
        
            csvfile_w = csv.DictWriter(csvfile, fieldnames=self.csvheader)
            if not isExist:
                csvfile_w.writeheader()
                csvfile.flush()

            self.csvfiles[key] = csvfile
            self.submap[key] = csvfile_w
        
        if self.submap.get(key, None) is not None:
            self.submap[key].writerows(data)
            self.csvfiles[key].flush()
            # self.csvfiles[id].flush()
        
        # 每隔5分钟，输出总共写了多少行，当前打开了多少个文件
        if datetime.now() > self.pre_print_info_dt + relativedelta(seconds=30):
            self.pre_print_info_dt = datetime.now()
            print(f"{datetime.now()},总计写文件的行：{self.already_write_row},当前打开的文件数量：{len(self.csvfiles)}")
    
    def write2csv_mix(self, ticks: List[TickData]):
        """不同的合约行情，都记在一个文件中"""
        data: list = []
        begin_time_data = datetime.now()
        for tick in ticks:
            '''
            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            d.pop("localtime")
            data.append(d)
            '''
            one_list: list = []
            one_list.append(tick.symbol)
            one_list.append(tick.exchange)
            one_list.append(tick.datetime)

            one_list.append(tick.name)
            one_list.append(tick.volume)
            one_list.append(tick.turnover)
            one_list.append(tick.open_interest)
            one_list.append(tick.last_price)
            one_list.append(tick.last_volume)
            one_list.append(tick.limit_up)
            one_list.append(tick.limit_down)

            one_list.append(tick.open_price)
            one_list.append(tick.high_price)
            one_list.append(tick.low_price)
            one_list.append(tick.pre_close)

            one_list.append(tick.bid_price_1)
            one_list.append(tick.bid_price_2)
            one_list.append(tick.bid_price_3)
            one_list.append(tick.bid_price_4)
            one_list.append(tick.bid_price_5)

            one_list.append(tick.ask_price_1)
            one_list.append(tick.ask_price_2)
            one_list.append(tick.ask_price_3)
            one_list.append(tick.ask_price_4)
            one_list.append(tick.ask_price_5)

            one_list.append(tick.bid_volume_1)
            one_list.append(tick.bid_volume_2)
            one_list.append(tick.bid_volume_3)
            one_list.append(tick.bid_volume_4)
            one_list.append(tick.bid_volume_5)

            one_list.append(tick.ask_volume_1)
            one_list.append(tick.ask_volume_2)
            one_list.append(tick.ask_volume_3)
            one_list.append(tick.ask_volume_4)
            one_list.append(tick.ask_volume_5)

            self.already_write_row += 1
            self.row_count += 1

            data.append(one_list)

        begin_time_write = datetime.now()

        self.csvfile_w.writerows(data)
        self.csvfile.flush()

        if self.row_count > self.max_row_one_file:
            self.csvfile.close()
            self.row_count = 0

            self.file_index += 1
            self.init_writer()
        
        end_time = datetime.now()
        print(f"{datetime.now()},总计写文件的行数：{self.already_write_row},当前写文件的行数：{len(ticks)},本次共耗时：{end_time-begin_time_data}，历遍耗时：{begin_time_write-begin_time_data},写入耗时：{end_time-begin_time_write}")
        