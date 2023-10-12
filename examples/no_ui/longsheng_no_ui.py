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
from vnpy_ib import IbGateway
from ls_quoterouter import QuoteRouterApp
from ls_quotecenter import QuoteCenterApp

from ls_common.object.object import Contract as LsContract
from ls_common.db.mysql_database import MysqlDatabase


SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True

CTP_SETTING = load_json("connect_ctp.json")
SOPT_SETTING = load_json("connect_sopt.json")
IB_SETTING = load_json("connect_ib.json")

# Chinese futures market trading period (day/night)
DAY_START = time(8, 40)
DAY_END = time(20, 00)

NIGHT_START = time(20, 40)
NIGHT_END = time(3, 00)


def check_trading_period():
    """"""
    current_time = datetime.now().time()

    trading = False
    # 符合开启的时间
    if (
        (current_time >= DAY_START and current_time <= DAY_END)
        or (current_time >= NIGHT_START)
        or (current_time <= NIGHT_END)
    ):
        trading = True
    
    # 周六、周日的白天，要关闭
    if datetime.now().isoweekday() in [6,7]:
        if current_time >= DAY_START and current_time <= DAY_END:
            trading = False
    # 周六、周日的晚上，也要关闭
    if datetime.now().isoweekday() in [6,7]:
        if current_time >= NIGHT_START:
            trading = False
    # 周日、周一的凌晨，也要关闭
    if datetime.now().isoweekday() in [1,7]:
        if current_time <= NIGHT_END:
            trading = False

    return trading


def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(CtpGateway)
    main_engine.add_gateway(IbGateway)
    
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")

    #main_engine.connect(CTP_SETTING, "CTP")
    #main_engine.write_log("连接CTP接口")
    # CTP初始化比较久，需要查询所有的合约信息
    #sleep(3)

    main_engine.connect(IB_SETTING, "IB")
    main_engine.write_log("连接IB接口")
    sleep(2)

    # 添加应用
    quoterouter_engine = main_engine.add_app(QuoteRouterApp)
    quote_center_engine = main_engine.add_app(QuoteCenterApp)

    quote_center_engine.start("tcp://*:2014", "tcp://*:4102")


    database: MysqlDatabase = MysqlDatabase()
    contract: LsContract = database.get_contract_by_symbol("HG2312-USD-FUT-COMEX")
    quote_center_engine.subscribe(contract)

    main_engine.write_log("VNPY录程序启动完成")

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
    print("VNPY守护父进程")

    child_process = None

    while True:
        trading = check_trading_period()

        # Start child process in trading period
        if trading and child_process is None:
            print("VNPY启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("VNPY子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive():
                child_process = None
                print("VNPY子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    run_parent()
