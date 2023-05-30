from typing import List
from datetime import datetime

from vnpy_mysql.mysql_database import MysqlDatabase, DbTickData, db, DateTimeMillisecondField
from vnpy.trader.object import BarData, TickData, ContractData
from vnpy.trader.database import convert_tz
from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    DoubleField,
    IntegerField,
    BooleanField,
    Model,
    MySQLDatabase as PeeweeMySQLDatabase,
    ModelSelect,
    ModelDelete,
    chunked,
    fn
)

class DbContractData(Model):
    """合约信息数据表映射对象"""

    id: AutoField = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    name: str = CharField()
    product: str = CharField()   #product: Product
    size: float = DoubleField()
    pricetick: float = DoubleField()

    min_volume: float = DoubleField()          # minimum trading volume of the contract
    stop_supported: bool = BooleanField()    # whether server supports stop order
    net_position: bool = BooleanField()      # whether gateway uses net position volume
    history_data: bool = BooleanField()      # whether gateway provides bar history data

    trading_hours: str = CharField(null = True)    # 交易时间
    time_zone: str = CharField(null = True)    # 品种所在的时区

    option_strike: float = DoubleField(null = True)
    option_underlying: str = CharField(null = True)     # vt_symbol of underlying contract
    option_type: str = CharField(null = True)   #option_type: OptionType
    option_listed: datetime = DateTimeMillisecondField(null = True)
    option_expiry: datetime = DateTimeMillisecondField(null = True)
    option_portfolio: str = CharField(null = True)
    option_index: str = CharField(null = True)          # for identifying options with same strike price

    class Meta:
        database: PeeweeMySQLDatabase = db
        indexes: tuple = ((("symbol", "exchange"), True),)


class LongShengMysqlDatabase(MysqlDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.db.create_tables([DbContractData])
    
    def save_bar_data(self, bars: List[BarData], stream: bool = False) -> bool:
        """父类是保存bar，我这里不需要保存bar"""
        pass

    def save_tick_data(self, ticks: List[TickData], stream: bool = False) -> bool:
        """子类改了父类2点。
            1、Tick不用按symbol分类，放在同一个list即可，在 whole market record engine中改
            2、不用管TickOverview，这里没有，反而增加入库开销
        """
        # 将TickData数据转换为字典，并调整时区
        data: list = []

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

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
    
    def save_contract_data(self, contracts: List[ContractData]):
        """增加记录合约信息"""
        data: list = []

        for contract in contracts:
            d: dict = contract.__dict__
            d["exchange"] = d["exchange"].value
            d["product"] = d["product"].value
            if d["option_type"] is not None:
                d["option_type"] = d["option_type"].value
            else:
                d["option_type"] = ""
            
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        # 使用upsert操作将数据更新到数据库中
        with self.db.atomic():
            for c in chunked(data, 200):
                DbContractData.insert_many(c).on_conflict_replace().execute()
