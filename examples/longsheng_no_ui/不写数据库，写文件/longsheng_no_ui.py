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
from whole_market_record_engine import WholeMarketRecorder
#from whole_market_record_engine_to_csv import WholeMarketRecorderToCsv

SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True

CTP_SETTING = load_json("connect_ctp.json")
SOPT_SETTING = load_json("connect_sopt.json")

# Chinese futures market trading period (day/night)
DAY_START = time(8, 40)
DAY_END = time(18, 00)

NIGHT_START = time(20, 40)
NIGHT_END = time(6, 30)


def check_trading_period():
    """"""
    current_time = datetime.now().time()

    trading = False
    if (
        (current_time >= DAY_START and current_time <= DAY_END)
        or (current_time >= NIGHT_START)
        or (current_time <= NIGHT_END)
    ):
        trading = True

    return trading


def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(CtpGateway)
    main_engine.add_gateway(SoptGateway)
    
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")

    whole_market_recorder = WholeMarketRecorder(main_engine, event_engine)
    #whole_market_recorder = WholeMarketRecorderToCsv(main_engine, event_engine)
    main_engine.write_log("录制模块已就绪")

    main_engine.connect(CTP_SETTING, "CTP")
    main_engine.write_log("连接CTP接口")
    # CTP初始化比较久，需要查询所有的合约信息
    sleep(3)

    main_engine.connect(SOPT_SETTING, "SOPT")
    main_engine.write_log("连接SOPT接口")
    # SOPT初始化比较久，需要查询所有的合约信息
    sleep(3)

    main_engine.write_log("行情记录程序启动完成")

    while True:
        sleep(10)

        trading = check_trading_period()
        if not trading:
            print("关闭子进程")
            main_engine.close()
            sys.exit(0)


def run_parent():
    """
    Running in the parent process.
    """
    print("启动行情记录守护父进程")

    child_process = None

    while True:
        trading = check_trading_period()

        # Start child process in trading period
        if trading and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive():
                child_process = None
                print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    run_parent()
