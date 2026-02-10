# 克隆自聚宽文章：https://www.joinquant.com/post/61912
# 标题：分享一个年化100低回撤的ETF轮动策略
# 作者：xiaolang1

# 克隆自聚宽文章：https://www.joinquant.com/post/57343
# 标题：多策略10.02社区学习版（更适合量化交易）
# 作者：O_iX

# 导入函数库
import math

from jqdata import *


# -------------------- 运行调度函数 --------------------
def initialize(context):

    set_option("avoid_future_data", True)  # 打开防未来函数
    set_option("use_real_price", True)  # 开启动态复权模式(真实价格)
    print("初始函数开始运行且全局只运行一次")  # 输出内容到日志 print()
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('strategy', 'debug')
    set_slippage(FixedSlippage(0.0001), type="fund")
    set_slippage(FixedSlippage(0.003), type="stock")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,
            open_commission=0.0003,
            close_commission=0.0003,
            close_today_commission=0,
            min_commission=5,
        ),
        type="stock",
    )
    # 设置货币ETF交易佣金0
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0,
            open_commission=0,
            close_commission=0,
            close_today_commission=0,
            min_commission=0,
        ),
        type="mmf",
    )
    # 全局变量
    g.strategys = {}
    g.portfolio_value_proportion = [1]  # 测试版
    g.positions = {i: {} for i in range(len(g.portfolio_value_proportion))}  # 记录每个子策略的持仓股票
    
    # 全天候权重
    g.weights = {}

    if g.portfolio_value_proportion[0] > 0:
        run_daily(etf_rotation_adjust, "11:00")
    # 每日剩余资金购买货币ETF
    run_daily(end_trade, "14:59")


def process_initialize(context):
    print("重启程序")
    g.strategys = {
        name: cls(context, index=idx, name=name)
        for name, cls, idx in [
            ("核心资产轮动策略", Etf_Rotation_Strategy, 0),
        ]
    }


# 尾盘处理
def end_trade(context):
    marked = {s for d in g.positions.values() for s in d}
    current_data = get_current_data()
    for stock in context.portfolio.positions:
        if stock not in marked:
            price = current_data[stock].last_price
            pos = context.portfolio.positions[stock].total_amount
            if my_order(stock, -pos, price, 0):
                print(f"卖出{stock}因送股未记录在持仓中", price, pos)

def my_order(security, vol, price, target_position):
    o = order(security, vol)
    return o


def etf_rotation_adjust(context):
    g.strategys["核心资产轮动策略"].adjust()

# -------------------- 策略基类 --------------------
class Strategy:

    def __init__(self, context, index, name):
        self.context = context
        self.index = index
        self.name = name
        self.stock_sum = 3
        self.hold_list = []
        self.min_money = 500  # 最小交易额(限制手续费)

    # 获取持仓市值
    def get_total_value(self):
        if not g.positions[self.index]:
            return 0
        return sum(self.context.portfolio.positions[key].price * value for key, value in g.positions[self.index].items())

    # 调仓(targets为字典，key为股票代码，value为目标市值)
    def _adjust(self, targets):
        current_data = get_current_data()

        # 输出信息
        strategy_data = {
            "strategy_name": self.name,
            "strategy_id": self.index,
            "stocks": [{"stock_name": current_data[stock].name, "stock_code": stock, "weight": weight} for stock, weight in targets.items()],
        }

        # print(strategy_data)

        # 获取已持有列表
        self.hold_list = list(g.positions[self.index].keys())
        portfolio = self.context.portfolio

        # 获取目标策略市值
        target_value = self.context.portfolio.total_value * g.portfolio_value_proportion[self.index]

        # 清仓被调出的
        for stock in self.hold_list:
            if stock not in targets:
                self.order_target_value_(stock, 0)

        # 先卖出
        for stock, weight in targets.items():
            target = target_value * weight
            price = current_data[stock].last_price
            value = g.positions[self.index].get(stock, 0) * price
            if value - target > max(self.min_money, price * 100):
                self.order_target_value_(stock, target)

        # 后买入
        for stock, weight in targets.items():
            target = target_value * weight
            price = current_data[stock].last_price
            value = g.positions[self.index].get(stock, 0) * price
            if min(target - value, portfolio.available_cash) > max(self.min_money, price * 100):
                self.order_target_value_(stock, target)

    # 自定义下单(涨跌停不交易)
    def order_target_value_(self, security, value):
        current_data = get_current_data()

        # 检查标的是否停牌、涨停、跌停
        if current_data[security].paused:
            print(f"{security}: 今日停牌")
            return False

        if current_data[security].last_price == current_data[security].high_limit:
            print(f"{security}: 当前涨停")
            return False

        if current_data[security].last_price == current_data[security].low_limit:
            print(f"{security}: 当前跌停")
            return False

        # 获取当前标的的价格
        price = current_data[security].last_price

        # 获取当前策略的持仓数量
        current_position = g.positions[self.index].get(security, 0)
        
        # 所有策略中持仓数量
        current_position_all = self.context.portfolio.positions[security].total_amount if security in self.context.portfolio.positions else 0

        # 计算目标持仓数量
        target_position = (int(value / price) // 100) * 100 if price != 0 else 0

        # 计算需要调整的数量
        adjustment = target_position - current_position
        
        target_position_all = current_position_all + adjustment

        # 检查是否当天买入卖出
        closeable_amount = self.context.portfolio.positions[security].closeable_amount if security in self.context.portfolio.positions else 0
        if adjustment < 0 and closeable_amount == 0:
            print(f"{security}: 当天买入不可卖出")
            return False

        # 下单并更新持仓
        if adjustment != 0:
            # o = order(security, adjustment)
            o = my_order(security, adjustment, price, target_position_all)
            if o:
                # 更新持仓数量
                filled = o.filled if o.is_buy else -o.filled
                g.positions[self.index][security] = filled + current_position
                # 如果当前持仓为零，移除该证券
                if g.positions[self.index][security] == 0:
                    g.positions[self.index].pop(security, None)
                # 更新持有列表
                self.hold_list = list(g.positions[self.index].keys())
                return True
        return False

    # 检查持仓中曾经涨停但当前未涨停的股票
    def _check(self):
        hold = list(g.positions[self.index].keys())
        if not hold:
            return []
        current_data = get_current_data()
        filtered = self.filter_limitup_stock(hold, 3)
        return [s for s in hold if s not in filtered and current_data[s].last_price < current_data[s].high_limit]

    # 识别无法交易的股票（停牌、涨跌停）
    def filter_untradeable_stock(self, stocks):
        current_data = get_current_data()
        return [
            stock
            for stock in stocks
            if current_data[stock].paused or current_data[stock].last_price in (current_data[stock].high_limit, current_data[stock].low_limit)
        ]

    # 根据调仓逻辑计算最终保留的股票列表
    def get_adjusted_stocks(self, selected, sell):
        fixed = self.filter_untradeable_stock(list(g.positions[self.index].keys()))
        sum = len(self.def_stocks) if selected == self.def_stocks else self.stock_sum - len(fixed)
        return fixed + [s for s in selected if s not in fixed and s not in sell][:sum]

# 核心资产轮动策略
class Etf_Rotation_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)

        self.stock_sum = 1
        self.weixing_pool = [
            "513130.XSHG",  # 恒生科技
            "510180.XSHG",
            "159915.XSHE",
            "512290.XSHG",
            "588120.XSHG",
            "515070.XSHG",
            
            "159851.XSHE",
            "159637.XSHE",
            "159550.XSHE",
            "512710.XSHG",
            "159692.XSHE"            
        ]
        self.etf_pool = [
            
            # 境外
            "513100.XSHG",  # 纳指ETF
            "513520.XSHG",  # 日经ETF
            "513030.XSHG",  # 德国ETF
            # 商品
            "518880.XSHG",  # 黄金ETF
            "159980.XSHE",  # 有色ETF
            "159985.XSHE",  # 豆粕ETF
            "501018.XSHG",  # 南方原油
            # 债券
            # "511090.XSHG",  # 30年国债ETF
            # 国内
            
        ] + self.weixing_pool
        
        self.m_days = 25  # 动量参考天数

    def filter(self):
        data = pd.DataFrame(index=self.etf_pool, columns=["annualized_returns", "r2", "score"])
        current_data = get_current_data()
        for etf in self.etf_pool:
            df = attribute_history(etf, self.m_days, "1d", ["close", "high"])
            prices = np.append(df["close"].values, current_data[etf].last_price)
            
            print(etf, 'prices', prices)

            # 设置参数
            y = np.log(prices)
            x = np.arange(len(y))
            weights = np.linspace(1, 2, len(y))

            # 计算年化收益率
            slope, intercept = np.polyfit(x, y, 1, w=weights)
            data.loc[etf, "annualized_returns"] = math.exp(slope * 250) - 1

            # 计算R²
            ss_res = np.sum(weights * (y - (slope * x + intercept)) ** 2)
            ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
            data.loc[etf, "r2"] = 1 - ss_res / ss_tot if ss_tot else 0

            # 计算得分
            data.loc[etf, "score"] = data.loc[etf, "annualized_returns"] * data.loc[etf, "r2"]

            # 过滤近3日跌幅超过5%的ETF
            if min(prices[-1] / prices[-2], prices[-2] / prices[-3], prices[-3] / prices[-4]) < 0.95:
                data.loc[etf, "score"] = 0

        # 过滤ETF，并按得分降序排列
        data = data.query("0 < score < 6").sort_values(by="score", ascending=False)
        print(data)

        return data.index.tolist()

    # 调仓
    def adjust(self):
        targets = self.filter()[: self.stock_sum]
        print(f"准备调仓: {targets}")
        self._adjust({etf: round(1 / len(targets), 3) for etf in targets})