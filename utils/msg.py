
import requests
import json

class Messager:
  def __init__(self):
    self.webhook1 = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6c1bd45a-74a7-4bd0-93ce-00b2e7157adc'
    self.webhook2 = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6c1bd45a-74a7-4bd0-93ce-00b2e7157adc'
  def send_message(self, webhook, message):
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
  # 发送消息

  def send_deal(self, dealInfo):
    stock = dealInfo['m_strProductName']
    price = dealInfo['m_dPrice']
    amount = dealInfo['m_dTradeAmount']
    markdown = f"""
    新增买入股票: <font color='warning'>{stock}</font>
    > 成交价: <font color='warning'>{price}/font>
    > 成交额: <font color='warning'>{amount}</font>
    """
    self.send_message(self.webhook1, markdown)

  def send_positions(self, positions):
    # stock = position['m_strProductName']
    df_result = pd.DataFrame(columns=['code', 'eps', 'market_cap'])
    for position in positions:
      df_result = df_result.append({
        'stock': position['m_strInstrumentName'],
        'price': position['m_dLastPrice'],
        'open_price': position['m_dOpenPrice'],
        'amount': position['m_dMarketValue'],
        'ratio': position['m_dProfitRate'],
        'profit': position['m_dFloatProfit'],
      }, ignore_index=True)

    markdown = """
    ## 📈 股票持仓报告
    """
    num = len(df_result)
    total_profit = df_result['profit'].sum()
    if total_profit > 0:
      total_profit = f"<font color='info'>{total_profit}%</font>"
    else:
      total_profit = f"<font color='warning'>-{total_profit}%</font>"
    
    for index, row in df_result.iterrows():
      row_str = self.get_position_markdown(row)
      markdown += row_str
    markdown += f"""
    ---
    **持仓统计**
    ▶ 总持仓数：`{num} 只`
    ▶ 总盈亏额：{total_profit}
    > 数据更新频率：每小时自动刷新
    """
    self.send_message(self.webhook2, markdown)
    
  def get_position_markdown(self, position):
    stock = position['stock']
    price = position['price']
    open_price = position['open_price']
    amount = position['amount']
    ratio = position['ratio']
    ratio_str = ratio * 100
    if ratio_str > 0:
      ratio_str = f"<font color='info'>{ratio_str}%</font>"
    else:
      ratio_str = f"<font color='warning'>-{ratio_str}%</font>"
    profit = position['profit']
    if profit > 0:
      profit = f"<font color='info'>{profit}%</font>"
    else:
      profit = f"<font color='warning'>-{profit}%</font>"
    return f"""
    ▪️ **{stock}**
    　├─ 当前价：`{price}`
    　├─ 成本价：`{open_price}`
    　├─ 持仓额：`¥{amount}`
    　├─ 盈亏率：`{ratio_str}`
    　└─ 盈亏额：`¥{profit}`
    """