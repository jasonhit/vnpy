from pathlib import Path

import importlib_metadata
from vnpy.trader.app import BaseApp
from vnpy.trader.object import (
    OrderData,
    TradeData,
    TickData,
    BarData
)

from .save_spread_data_engine import (
    SaveSpreadDataEngine,
    APP_NAME
)


try:
    __version__ = importlib_metadata.version("ls_savespreaddata")
except importlib_metadata.PackageNotFoundError:
    __version__ = "dev"


class SaveSpreadDataApp(BaseApp):
    """"""

    app_name: str = APP_NAME
    app_module: str = __module__
    app_path: Path = Path(__file__).parent
    display_name: str = "价差数据存储"
    engine_class: SaveSpreadDataEngine = SaveSpreadDataEngine
    widget_name: str = "SpreadManager"
    icon_name: str = str(app_path.joinpath("ui", "spread.ico"))
