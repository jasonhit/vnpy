import csv
from typing import List,Dict, Any
import os
import sys
from datetime import datetime,time
from pathlib import Path
from io import TextIOWrapper
from dateutil.relativedelta import relativedelta
from collections import Counter

from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange
from vnpy.trader.utility import TRADER_DIR

class MarketDadaCsvStore:
    def __init__(self):
        self.csvfiles_map: Dict[str, TextIOWrapper] = {} # compose_name: csvfile
        self.csvwriters_map: Dict[str, Any] = {} # compose_name: csvwriter          

    def write2csv(self, spread_datas: List):
        """不同的组合行情，记在不同的文件中，list的元素是3元组(compse_name, compese_head, compese_data)"""

        # 先把spread_datas按照合约名称分组
        spread_datas_dict = {}
        spread_heads_dict = {}
        for spread_data in spread_datas:
            compose_name, compose_head, compose_data = spread_data
            if compose_name not in spread_datas_dict:
                spread_datas_dict[compose_name] = []
                spread_heads_dict[compose_name] = compose_head
            spread_datas_dict[compose_name].append(compose_data)

        # 再把spread_datas_dict 按名称，写入不同的csv文件中
        for key, value in spread_datas_dict.items():
            self.write2csv_by_name(key, value, spread_heads_dict[key])


    def write2csv_by_name(self, key: str, data: List, head: str):
        # 检查文件是否已经打开
        if self.csvfiles_map.get(key, None) is None:
            today_str = datetime.now().strftime("%Y%m%d")
            csvname = f"{key}-spread-data-{today_str}.csv"
            
            dir_path = TRADER_DIR.joinpath("spread_data")
            dir_path = dir_path.joinpath(key)   
            dir_path.mkdir(parents=True, exist_ok=True)
            #print(f"文件目录：{dir_path}")
            
            file_path = dir_path.joinpath(csvname)
            isExist = os.path.isfile(file_path)
            csvfile = open(file_path, 'a+', newline='')
            csvfile_w = csv.writer(csvfile)
            if not isExist:
                csvfile_w.writerow(head.split(","))
                csvfile.flush()

            self.csvfiles_map[key] = csvfile
            self.csvwriters_map[key] = csvfile_w

        # 写入data数据到csv文件中
        if self.csvwriters_map.get(key, None) is not None:
            for value in data:
                self.csvwriters_map[key].writerow(value.split(","))            
            self.csvfiles_map[key].flush()          
        
    
    def write2csv_mix(self, ticks: List[TickData]):
        """不同的合约行情，都记在一个文件中"""
        pass      