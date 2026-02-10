from jqdata import *


def initialize(context):
    set_option('use_real_price', True)
    # 设定监测对象
    g.spot_index = '000852.XSHG'  # 中证1000现货指数
    g.future_symbol = 'IM'        # 中证1000股指期货代码
    
    # 每天 14:45 运行，此时当天的涨跌格局已定，方便尾盘出逃
    run_daily(record_im_basis, time='14:45')

def record_im_basis(context):
  today = context.current_dt.strftime('%Y-%m-%d')
  
  # 获取包括今天在内的过去3个交易日
  trade_days = get_trade_days(end_date=today, count=3)
  
  basis_rates = []
  
  # 遍历日期计算基差率
  for date in trade_days:
      date_str = date.strftime('%Y-%m-%d')
      
      # 如果是今天，使用实时数据
      if date_str == today:
          main_contract = get_dominant_future(g.future_symbol, date=today)
          if main_contract:
              current_data = get_current_data()
              spot_price = current_data[g.spot_index].last_price
              future_price = current_data[main_contract].last_price
              if spot_price > 0:
                  rate = (future_price / spot_price - 1) * 100
                  basis_rates.append(rate)
      else:
          # 历史数据：获取当时的主力合约和收盘价
          dom_future = get_dominant_future(g.future_symbol, date=date_str)
          if dom_future:
              spot_df = get_price(g.spot_index, end_date=date, count=1, frequency='daily', fields=['close'])
              future_df = get_price(dom_future, end_date=date, count=1, frequency='daily', fields=['close'])
              if not spot_df.empty and not future_df.empty:
                  s_close = spot_df['close'].iloc[0]
                  f_close = future_df['close'].iloc[0]
                  if s_close > 0:
                      rate = (f_close / s_close - 1) * 100
                      basis_rates.append(rate)
  
  if not basis_rates:
      return False
      
  # 计算加权平均
  # 如果数据不足3天，就用现有的数据平均
  if len(basis_rates) == 1:
      avg_rate = basis_rates[0]
  elif len(basis_rates) == 2:
      # 昨天 0.4, 今天 0.6
      avg_rate = basis_rates[0] * 0.4 + basis_rates[1] * 0.6
  else:
      # 前天 0.2, 昨天 0.3, 今天 0.5 (或者 1:2:3 加权)
      # 使用 1:2:3 加权: (r1*1 + r2*2 + r3*3) / 6
      avg_rate = (basis_rates[0] * 1 + basis_rates[1] * 2 + basis_rates[2] * 3) / 6.0

  # 4. 绘图与记录
  record(IM_Basis_Rate = avg_rate)  # 绘制基差率曲线
  record(Zero_Line = 0)               # 0轴参考线
  record(Panic_Line = -1.5)           # 恐慌参考线（经验值：贴水超1.5%通常意味着异动）

  # 5. 辅助对冲压力计算：计算基差的偏离度
  # 获取过去 20 天的基差数据，判断当前是否属于“异常贴水”
  # 此处逻辑可根据需要开启，用于日志报警
  if basis_rate < -2.0:
      log.warn(">>> ⚠️ IM基差异常：当前贴水率 %.2f%%，主力合约: %s，对冲压力巨大！" % (basis_rate, main_contract))
 

def handle_data(context, data):
  pass