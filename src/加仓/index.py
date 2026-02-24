#encoding:gbk
'''
本策略事先设定好交易的股票篮子，然后根据指数的CCI指标来判断超买和超卖
当有超买和超卖发生时，交易事先设定好的股票篮子
'''
import pandas as pd
import numpy as np
import talib

def init(ContextInfo):
    print(is_trading(ContextInfo))
    

def is_trading(ContextInfo):
    return ContextInfo.get_instrumentdetail('600000.SH')['IsTrading'] or ContextInfo.get_instrumentdetail('600036.SH')['IsTrading'] or ContextInfo.get_instrumentdetail('600519.SH')['IsTrading']