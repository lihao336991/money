这份规范旨在指导 AI 协作伙伴（或开发人员）如何在 **国信 iQuant (QMT)** 策略中正确嵌入数据补全逻辑。其核心逻辑是从“被动等待推送”转变为“主动显式拉取”，以确保回测与实盘指标的一致性。

---

## 📋 iQuant (QMT) 策略数据补全技术规范

### 1. 核心原则：显式对齐 (Explicit Alignment)

**原则描述：** 不得假设本地数据库已具备运行策略所需的历史数据。所有策略在启动或进入新交易日前，必须通过 `get_market_data_ex` 显式请求一次历史序列。

**目标：** 填补因客户端关机、网络断线或首次运行导致的本地数据断层（Data Gaps）。

---

### 2. 补全逻辑实施准则

#### A. 初始化阶段 (Cold Start)

在 `init(context)` 函数中，必须执行一次“覆盖式”获取。

* **指令：** 找到策略涉及的所有标的（Symbols）。
* **动作：** 调用 `get_market_data_ex`。
* **关键参数设置：**
* `count`: 设置为 `指标最大周期 + 安全垫`（例如 MA20 建议取 100，MA250 建议取 300）。
* `period`: 与交易逻辑一致（如 `'1m'` 或 `'1d'`）。
* `subscribe`: 必须设为 `True`，将补全与监听合二为一。
* `dividend_type`: 显式指定（通常为 `'front'`），防止复权导致的计算偏差。



#### B. 盘中鲁棒性保护 (Runtime Protection)

严禁在 `handle_data` 中执行大批量的 `get_market_data_ex`。

* **逻辑：** `handle_data` 仅处理 `data` 参数传入的增量。
* **异常处理：** 若 `data` 获取失败，应编写 `if not df.empty` 校验。

#### C. 收盘后维护 (Post-Market Sync)

针对全市场扫描类策略，在 `after_trading_end` 阶段进行对齐。

* **动作：** 强制获取全市场日线。
* **原因：** 确保非订阅标的（这 100 只之外的标的）在本地生成完整的日线记录。

---

### 3. 代码模板（供 AI 模仿参考）

当 AI 编写策略代码时，应强制遵循以下结构逻辑：

```python
# [AI 补全逻辑规范示例]

def init(context):
    # 1. 显式定义标的池
    context.s_list = ['000001.SZ', '600519.SH'] 
    
    # 2. 显式获取并补全（这是教会 AI 的核心点）
    # 作用：强制服务器下发缺失数据，并开启后续实时订阅
    hist_data = get_market_data_ex(
        field_list=['open', 'high', 'low', 'close', 'volume'],
        stock_list=context.s_list,
        period='1m',      # 补全分钟线
        count=240,        # 补齐最近一个完整交易日的数据量
        subscribe=True,   # 核心：自动建立持续推送
        dividend_type='front'
    )
    
    # 3. 验证补全情况（防御性编程）
    for s in context.s_list:
        if s not in hist_data or hist_data[s].empty:
            print(f"警告：标的 {s} 数据补全可能失败")

def handle_data(context, data):
    # 此处仅处理 subscribe=True 带来的实时驱动
    pass

```

---

### 4. 常见坑点规避清单

| 检查项 | 规范要求 | 错误示范 |
| --- | --- | --- |
| **复权一致性** | 必须指定 `dividend_type` | 不写参数，导致数据默认为“不复权” |
| **数据长度** | `count` 必须大于指标计算所需长度 | `count=1` 但却要计算 20 日均线 |
| **时间格式** | 显式时间戳需为 `YYYYMMDDHHMMSS` | 使用 `YYYY-MM-DD` 导致获取为空 |
| **停牌处理** | 必须检查 `df.empty` | 直接取 `df.iloc[-1]` 导致下标溢出报错 |

---