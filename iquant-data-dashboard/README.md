# iQuant 数据完整度大盘

独立的本机 iQuant / QMT 数据完整度大盘。它按你选择的时间段、基准池和数据类型，计算每天的数据覆盖率：

- 某天所有标的都有数据：覆盖率 100%。
- 某天一半标的有数据：覆盖率 50%。
- 某天完全没有数据：覆盖率 0%。

主视图提供覆盖率折线图、日期热力图和低覆盖日期列表；标的明细用于定位具体缺口。

## 启动

```powershell
cd D:\codes\money\iquant-data-dashboard
python server.py --host 127.0.0.1 --port 8787
```

打开：

```text
http://127.0.0.1:8787
```

## 页面可调条件

- 开始日期 / 结束日期。
- 基准池：沪深A股、上证A股、深证A股、中小综指、常用ETF、核心指数。
- 数据类型：日线K线、1分钟K线、5分钟K线、财务数据。

## 数据来源

服务启动后会优先尝试导入：

```python
from xtquant import xtdata
```

如果当前 Python 环境无法导入 `xtquant`，页面会显示 `Demo 数据`，用于验证界面和交互。放到 iQuant/QMT 可用的 Python 环境里启动后，会自动切换为 `真实 xtquant 数据`。

## 配置

编辑 `dashboard_config.json`：

- `universes`：基准池。
- `data_types`：数据类型及对应 iQuant 周期。
- `default_days`：默认回看天数。
- `fallback_codes`：无法读取板块时用于演示或兜底的代码。

## 注意

分钟线当前按“某个交易日是否有该标的数据”计算日级覆盖率，不展开到日内每一分钟 bar 的完整率。财务数据在不同券商 iQuant 环境里的接口差异较大，当前保留了可插拔入口；如果当前 `xtdata` 环境没有财务方法，会回退到 Demo 覆盖率。
