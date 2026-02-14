AI量化Coding Skill 规范 (QMT版)

1. 代码基础配置与环境兼容
编码规范： 脚本首行必须声明 #coding:gbk，以适配QMT在Windows环境下的中文解析。

动静分离架构： * 使用全局类 G 或平台自带的 context 存储状态。

回测/实盘双模运行： 必须通过 context.do_back_test 判断运行环境。

回测： 使用 context.runner.run_daily 注册自定义任务。

实盘： 必须使用 context.run_time 注册定时任务，且参数需符合 1nDay 或 7nDay 格式。

2. QMT API 调用标准
行情获取： 优先使用 get_market_data_ex 进行批量行情获取，必须设置 dividend_type="follow" (复权) 及 subscribe=True。

财务数据： 使用 get_raw_financial_data 获取财报。需注意该接口返回的是嵌套字典结构，必须通过 list(eps[code][字段].values())[-1] 的方式获取最新值，并加入 try-except 捕获数据缺失异常。

下单指令：

回测： 使用 order_target_percent 或 order_target_value。

实盘： 必须使用 passorder。

买入： 模式 23 (代码下单)，指令 1102 (总金额)，价格模式 5 (最新价)。

卖出： 模式 24 (代码下单)，指令 1123 (可用持仓比例)，比例传 1 表示全卖。

账户信息： 实盘获取资金使用 get_trade_detail_data(account, 'stock', 'account') 并遍历 m_dAvailable 字段。

3. 定时任务与流程控制
任务原子化： 每一个调度任务需对应一个顶层包装函数（如 sell_stocks_func），严禁在 run_time 中直接使用 lambda。

4. 工具类集成规范
TimeManager： 统一使用 TimeManager 类管理时间。禁止直接调用 datetime.now()，实盘需通过 get_bar_timetag 获取北京时间。

Storage： 凡是跨日期需要持久化的变量（如 target_list），必须通过 Storage 类写入 .txt 缓存文件。

Messager： 所有核心动作（买入、卖出、清仓、异常）必须通过微信 Webhook 发送 Markdown 格式通知。