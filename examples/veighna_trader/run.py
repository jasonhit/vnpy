# flake8: noqa
from time import sleep

from vnpy.event import EventEngine

from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_ctp import CtpGateway
# from vnpy_ctptest import CtptestGateway
# from vnpy_mini import MiniGateway
# from vnpy_femas import FemasGateway
from vnpy_sopt import SoptGateway
# from vnpy_sec import SecGateway
# from vnpy_uft import UftGateway
# from vnpy_esunny import EsunnyGateway
# from vnpy_xtp import XtpGateway
# from vnpy_tora import ToraStockGateway
# from vnpy_tora import ToraOptionGateway
# from vnpy_comstar import ComstarGateway
from vnpy_ib import IbGateway
# from vnpy_tap import TapGateway
# from vnpy_da import DaGateway
# from vnpy_rohon import RohonGateway
# from vnpy_tts import TtsGateway
# from vnpy_ost import OstGateway
from vnpy_hft import HftGateway

# from vnpy_paperaccount import PaperAccountApp
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctabacktester import CtaBacktesterApp
# from vnpy_spreadtrading import SpreadTradingApp
from vnpy_algotrading import AlgoTradingApp
# from vnpy_optionmaster import OptionMasterApp
# from vnpy_portfoliostrategy import PortfolioStrategyApp
# from vnpy_scripttrader import ScriptTraderApp
# from vnpy_chartwizard import ChartWizardApp
# from vnpy_rpcservice import RpcServiceApp
# from vnpy_excelrtd import ExcelRtdApp
from vnpy_datamanager import DataManagerApp
# from vnpy_datarecorder import DataRecorderApp
# from vnpy_riskmanager import RiskManagerApp
# from vnpy_webtrader import WebTraderApp
# from vnpy_portfoliomanager import PortfolioManagerApp
from vnpy_ls_gridtrading import GridStrategyApp

def main():
    """"""
    qapp = create_qapp()

    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(CtpGateway)
    # main_engine.add_gateway(CtptestGateway)
    # main_engine.add_gateway(MiniGateway)
    # main_engine.add_gateway(FemasGateway)
    main_engine.add_gateway(SoptGateway)
    # main_engine.add_gateway(SecGateway)    
    # main_engine.add_gateway(UftGateway)
    # main_engine.add_gateway(EsunnyGateway)
    # main_engine.add_gateway(XtpGateway)
    # main_engine.add_gateway(ToraStockGateway)
    # main_engine.add_gateway(ToraOptionGateway)
    # main_engine.add_gateway(OesGateway)
    # main_engine.add_gateway(ComstarGateway)
    main_engine.add_gateway(IbGateway)
    # main_engine.add_gateway(TapGateway)
    # main_engine.add_gateway(DaGateway)
    # main_engine.add_gateway(RohonGateway)
    # main_engine.add_gateway(TtsGateway)
    # main_engine.add_gateway(OstGateway)
    # main_engine.add_gateway(NhFuturesGateway)
    # main_engine.add_gateway(NhStockGateway)
    main_engine.add_gateway(HftGateway)

    # main_engine.add_app(PaperAccountApp)
    #main_engine.add_app(CtaStrategyApp)
    #main_engine.add_app(CtaBacktesterApp)
    # main_engine.add_app(SpreadTradingApp)
    #main_engine.add_app(AlgoTradingApp)
    # main_engine.add_app(OptionMasterApp)
    # main_engine.add_app(PortfolioStrategyApp)
    # main_engine.add_app(ScriptTraderApp)
    # main_engine.add_app(ChartWizardApp)
    # main_engine.add_app(RpcServiceApp)
    # main_engine.add_app(ExcelRtdApp)
    #main_engine.add_app(DataManagerApp)
    # main_engine.add_app(DataRecorderApp)
    # main_engine.add_app(RiskManagerApp)
    # main_engine.add_app(WebTraderApp)
    # main_engine.add_app(PortfolioManagerApp)
    grid_engine = main_engine.add_app(GridStrategyApp)

    
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    
    '''
    # 程序自动连接api
    main_window.connect_gateway("IB")
    sleep(2)
    main_window.connect_gateway("CTP")
    sleep(30)

    # 程序自动打开网格交易
    main_window.open_grid_strategy()
    # 初始化并启动所有网格策略
    grid_engine.init_all_strategies()
    sleep(15)
    grid_engine.start_all_strategies()

    # 指定时间后，自动关闭程序
    main_window.auto_close_program(15, 40, 0)
    '''

    qapp.exec()


if __name__ == "__main__":
    main()
