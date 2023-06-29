import multiprocessing
import sys,os
from time import sleep
from datetime import datetime, time
from logging import INFO

from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
from vnpy.trader.utility import load_json

from vnpy_ctp import CtpGateway
from vnpy_sopt import SoptGateway

from vnpy_spreadtrading import SpreadTradingApp
from vnpy_spreadtrading.base import EVENT_SPREAD_LOG

from ls_savespreaddata import SaveSpreadDataApp

SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True

CTP_SETTING = load_json("connect_ctp.json")
SOPT_SETTING = load_json("connect_sopt.json")

def main():
    """"""
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(CtpGateway)
    main_engine.add_gateway(SoptGateway)
    spread_engine = main_engine.add_app(SpreadTradingApp)
    save_spread_data_engine = main_engine.add_app(SaveSpreadDataApp)

    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_SPREAD_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")

    main_engine.connect(CTP_SETTING, "CTP")
    main_engine.write_log("连接CTP接口")
    # CTP初始化比较久，需要查询所有的合约信息
    sleep(3)

    #main_engine.connect(SOPT_SETTING, "SOPT")
    #main_engine.write_log("连接SOPT接口")
    # SOPT初始化比较久，需要查询所有的合约信息
    #sleep(3)

    spread_engine.start()
    save_spread_data_engine.start()

    main_engine.write_log("价差记录程序启动完成")




if __name__ == "__main__":
    main()