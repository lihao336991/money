#encoding:gbk
import datetime
from datetime import datetime, time, timedelta

import numpy as np
import requests
import pandas as pd

import time as nativeTime
import json
# ====================================================================
# 【全局配置】
# ====================================================================
class G():pass
g = G()


# ====================================================================
# 【核心逻辑】
# ====================================================================

def init(context):
    # 实盘定时任务：每天 14:55 执行
    # if not context.do_back_test:
    # context.run_time("record_smoothed_basis", "1nDay", "2025-01-01 14:55:00", "SH")
    print(">>> 7日加权基差监控已启动")
        
    # 初始化时间管理器
    context.tm = TimeManager(context)
    context.runner = TaskRunner(context)
    context.run_time("record_smoothed_basis","1nDay","2025-03-01 14:55:00","SH")

g.window = 7                # 监控基差 7日窗口

def record_smoothed_basis(context):
    # 1. 直接获取连续主力合约代码 (规避换月数据断层)
    # 备注：IML0 是中金所 IM 连续主力
    main_continuous = 'IM.IF'
    main_stock = '000852.SH'  # 中证1000指数
    
    stock_list=[main_stock, main_continuous]
    # 2. 获取数据 (增加 count 以确保对齐后仍有足够窗口)
    price_data = context.get_market_data_ex(
        [],
        stock_list,
        period='1m',
        start_time = context.tm.date_str,
        end_time = context.tm.date_str,
        count=g.window + 5,
        dividend_type = "follow",
        fill_data = False,
        subscribe = True
    )

    tick_data = context.get_full_tick([main_continuous])

    print(f"时间: {context.tm.now} 获取到的主力连续数据: {price_data[main_continuous]}")
    # print(f"最新tick数据： {tick_data[main_continuous]}")
    
    if main_stock not in price_data or main_continuous not in price_data:
        return

    # 3. 使用 Pandas 对齐数据
    df_idx = price_data[main_stock][['close']].rename(columns={'close': 'idx_close'})
    df_fut = price_data[main_continuous][['close']].rename(columns={'close': 'fut_close'})
    
    # 按时间戳合并
    df_merged = df_idx.join(df_fut, how='inner').dropna()
    
    if len(df_merged) < g.window:
        return

    # 4. 计算基差序列 (取最后 window 天)
    df_merged = df_merged.tail(g.window)
    # 基差 = (期货 / 现货 - 1) * 100
    df_merged['basis'] = (df_merged['fut_close'] / df_merged['idx_close'] - 1) * 100
    
    # 5. 计算 WMA
    weights = np.arange(1, g.window + 1)
    wma_basis = np.sum(df_merged['basis'].values * weights) / weights.sum()
    curr_basis = df_merged['basis'].iloc[-1]

    print(f"主力连续: {main_continuous} | 实时基差: {curr_basis:.2f}% | 7日加权: {wma_basis:.2f}%")
    
    # 逃顶
    if curr_basis < -2:
        print(f"逃顶: {wma_basis}")
        messager.sendLog(f"主力连续: {main_continuous} | 实时基差: {curr_basis:.2f}% | 期货价格：{df_fut.iloc[-1]['fut_close']:.2f}， 现货价格：{df_idx.iloc[-1]['idx_close']:.2f}")
        messager.sendLog("📢📢📢📢📢 重大风险清仓 !!! 📢📢📢📢📢")
        messager.sendLog("📢📢📢📢📢 重大风险清仓 !!! 📢📢📢📢📢")
        messager.sendLog("📢📢📢📢📢 重大风险清仓 !!! 📢📢📢📢📢")
    else:
        messager.sendLog(f"基差：{curr_basis:.2f}%，没有风险")

def checkTask(context):
    context.runner.check_tasks(context.tm.now)

# 在handlebar函数中调用（假设当前K线时间戳为dt）
def handlebar(context):
    try:
        # 更新时间管理器状态
        context.tm.update()
        
        # 保持兼容性，同步旧的时间变量
        context.currentTime = context.tm.timestamp
        context.today = context.tm.now
        
        # 回测模式下需要手动触发任务检查
        if context.do_back_test:
            # 检查并执行任务
            context.runner.check_tasks(context.tm.now)
            
    except Exception as e:
        print('handlebar异常', e)
        import traceback
        traceback.print_exc()
        


class ScheduledTask:
    """定时任务基类"""
    def __init__(self, execution_time):
        self.last_executed = None
        self.execution_time = self._parse_time(execution_time)
    
    def _parse_time(self, time_str):
        """将HH:MM格式字符串转换为time对象"""
        try:
            return datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError("Invalid time format, use HH:MM")

class MinuteTask(ScheduledTask):
    """分钟级别任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = current_dt - timedelta(minutes=1) >= self.last_executed        
        should = should1 and should2
        # 当前时间已过执行时间 且 超过1分钟
        return should

class DailyTask(ScheduledTask):
    """每日任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = self.last_executed != current_dt.date()
        should = should1 and should2
        # 当前时间已过执行时间 且 当日未执行
        return should

class WeeklyTask(ScheduledTask):
    """每周任务"""
    def __init__(self, weekday, execution_time):
        super().__init__(execution_time)
        self.weekday = weekday  # 0-6 (周一至周日)
    
    def should_trigger(self, current_dt):
        should1 = int(current_dt.weekday()) == self.weekday
        should2 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        week_num = current_dt.isocalendar()[1]        
        should3 = self.last_executed != f"{current_dt.year}-{week_num}"
        should = should1 and should2 and should3
        # if should:
        #     print('每周调仓时间到', current_dt)
        # 周几匹配 且 时间已过 且 当周未执行
        return should

class TaskRunner:
    def __init__(self, context):
        self.daily_tasks = []
        self.weekly_tasks = []
        self.context = context

    def run_daily(self, time_str, task_func):
        """注册每日任务
        Args:
            time_str: 触发时间 "HH:MM"
            task_func: 任务函数
        """
        self.daily_tasks.append( (DailyTask(time_str), task_func) )
    
    def run_weekly(self, weekday, time_str, task_func):
        """注册每周任务
        Args:
            weekday: 0-6 代表周一到周日
            time_str: 触发时间 "HH:MM"
            task_func: 任务函数
        """
        self.weekly_tasks.append( (WeeklyTask(weekday, time_str), task_func) )
    
    def check_tasks(self, bar_time):
        """在handlebar中调用检查任务
        Args:
            bar_time: K线结束时间(datetime对象)
        """
        # 处理每日任务
        for task, func in self.daily_tasks:
            if task.should_trigger(bar_time):
                func(self.context)
                task.last_executed = bar_time.date()
        
        # 处理每周任务
        for task, func in self.weekly_tasks:
            if task.should_trigger(bar_time):
                func(self.context)
                week_num = bar_time.isocalendar()[1]
                task.last_executed = f"{bar_time.year}-{week_num}"  # (year, week)


class TimeManager:
    """
    统一时间管理类
    解决实盘和回测中时间对象(datetime)与时间戳(timestamp)管理混乱的问题
    """
    def __init__(self, context):
        self.context = context
        self._timestamp = 0
        self._dt = datetime.now()
        # 初始化时间
        self.update(init=True)

    def update(self, init=False):
        """更新当前时间状态"""
        if init and not self.context.do_back_test:
            # 实盘初始化时使用系统时间
            self._timestamp = nativeTime.time() * 1000 + 8 * 3600 * 1000
            self._dt = pd.to_datetime(self._timestamp, unit='ms')
            print('TimeManager初始化时间:', self._timestamp)
            return

        # 获取当前K线时间
        index = self.context.barpos
        # get_bar_timetag返回的是毫秒时间戳，通常需要加8小时转北京时间
        current_k_time = self.context.get_bar_timetag(index) + 8 * 3600 * 1000
        
        if not self.context.do_back_test:
            # 实盘模式：只在时间推进时更新（过滤掉旧的K线数据）
            if self._timestamp < current_k_time:
                self._timestamp = current_k_time
                self._dt = pd.to_datetime(self._timestamp, unit='ms')
        else:
            # 回测模式：直接更新
            self._timestamp = current_k_time
            self._dt = pd.to_datetime(self._timestamp, unit='ms')

    @property
    def now(self) -> datetime:
        """获取当前datetime对象"""
        return self._dt

    @property
    def timestamp(self) -> float:
        """获取当前时间戳(毫秒)"""
        return self._timestamp

    @property
    def date_str(self) -> str:
        """获取YYYYMMDD格式日期字符串"""
        return self._dt.strftime('%Y%m%d')
    
    @property
    def time_str(self) -> str:
        """获取HH:MM:SS格式时间字符串"""
        return self._dt.strftime('%H:%M:%S')

    @property
    def year(self) -> int:
        return self._dt.year

    @property
    def month(self) -> int:
        return self._dt.month
        
    @property
    def day(self) -> int:
        return self._dt.day
        
    @property
    def weekday(self) -> int:
        """返回星期几 (0=周一, 6=周日)"""
        return self._dt.weekday()

    def get_past_date(self, days: int) -> str:
        """获取过去N天的日期字符串(YYYYMMDD)"""
        return (self._dt - timedelta(days=days)).strftime('%Y%m%d')


class Messager:
    is_test = False
    def __init__(self):
        # 消息通知
        self.webhook1 = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e861e0b4-b8e2-42ed-a21a-1edf90c41618'
    def set_is_test(self, is_test):
        self.is_test = is_test
    def send_message(self, webhook, message):
        if self.is_test:
            return
        # 设置企业微信机器人的Webhook地址
        headers = {'Content-Type': 'application/json; charset=UTF-8'}
        data = {
            'msgtype': 'markdown', 
            'markdown': {
                'content': message
            }
        }
        response = requests.post(webhook, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            print('消息发送成功')
        else:
            print('消息发送失败')
    # 发送消息（支持控制只在开盘期间推送）
    def sendLog(self, message):
        if is_trading():
            self.send_message(self.webhook1, message)
        print(message)


messager = Messager()



def is_trading(ContextInfo):
    current_time = datetime.now().time()
    return time(9,0) <= current_time <= time(16,0)
