# 导入CtaTemplate模板
from vnpy_ctastrategy import (
CtaTemplate,
BarGenerator,
ArrayManager
)

# 定义根据价格来做网格的交易策略类
class PriceGridStrategy(CtaTemplate):
    # 定义策略参数
    author = "Bing"
    grid_size = 0.01 # 网格大小，百分比
    grid_num = 10 # 网格数量
    fixed_size = 100 # 每次交易数量

    parameters = ["grid_size", "grid_num", "fixed_size"]
    variables = []

    # 构造函数
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # 初始化网格价格列表
        self.grid_prices = []

        # 初始化持仓状态
        self.pos_state = 0 # -grid_num ~ grid_num

    # 处理Tick数据
    def on_tick(self, tick):
        self.cancel_all() # 撤销所有委托单

        # 如果网格价格列表为空，就根据当前价格生成网格价格列表
        if not self.grid_prices:
            price = tick.last_price # 获取当前最新价
            upper = price * (1 + self.grid_size * self.grid_num / 2) # 计算网格上限价
            lower = price * (1 - self.grid_size * self.grid_num / 2) # 计算网格下限价
            step = (upper - lower) / (self.grid_num - 1) # 计算网格间隔

        for i in range(self.grid_num):
            grid_price = lower + i * step # 计算每个网格的价格
            self.grid_prices.append(grid_price) # 添加到网格价格列表

        # 根据当前持仓状态和价格所在的网格位置，决定买卖操作
        for i in range(self.grid_num):
            grid_price = self.grid_prices[i] # 获取每个网格的价格

        if tick.last_price > grid_price: # 如果当前价格高于网格价格
            if self.pos_state < i - self.grid_num / 2: # 如果当前持仓状态低于网格位置
                self.sell(grid_price, self.fixed_size, True) # 以网格价格卖出
                self.pos_state += 1 # 更新持仓状态

        elif tick.last_price < grid_price: # 如果当前价格低于网格价格
            if self.pos_state > i - self.grid_num / 2 - 1: # 如果当前持仓状态高于网格位置
                self.buy(grid_price, self.fixed_size, True) # 以网格价格买入
                self.pos_state -= 1 # 更新持仓状态

        self.put_event() # 发出策略状态更新事件

    # 处理成交数据
    def on_trade(self, trade):
        pass

    # 处理委托数据
    def on_order(self, order):
        pass

    # 处理停止单（本策略没有用到）
    def on_stop_order(self, stop_order):
        pass