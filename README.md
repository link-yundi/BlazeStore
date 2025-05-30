# BlazeStore 
🚀 blazestore —— The blazing-fast data toolkit for quantitative workflows
---
专注于本地量化数据的高效管理与读写，具备以下特点：
- High Performance：借助 polars（Rust 实现），大幅优于 pandas，单机内存/多核利用率高，I/O 高效，支持宽表大数据量（TB 级别）分析。
- 分区与列式存储：自动按日期等分区，底层 Parquet 格式，适合全频段（tick/分钟/日线）数据。
- 支持本地高效的数据读写、SQL 查询、分区管理，并方便与主流数据库（MySQL、ClickHouse）集成。
- 内置任务调度与批量更新（DataUpdater），适合日常行情和因子数据自动维护。
- 支持因子工程，便于复用、管理、批量计算和依赖关系控制，适合复杂因子体系的量化研究。

### Installation
```bash
pip install -U blazestore
```

### QuickStart
```python
import blazestore as bs

# 获取配置
bs.get_settings()

# 假设有一个polars.DataFrame df, 内容为分钟频数据
kline_df = ... # date | time | asset | open | high | low | close | volume

# 持久化, 存放在表格 market_data/kline_minute, 按照日期分区
tb_name = "market_data/kline_minute"
bs.put(kline_df, tb_name=tb_name, partitions=["date", ],)
print((bs.DB_PATH/tb_name).exists()) # True

# read local data
query = f"select * from {tb_name} where date = '2025-05-06';"

read_df = bs.sql(query)
```

### Examples
#### 1.update data

```python
import blazestore as bs

# implement update function
def update_stock_kline_day(tb_name, date):
    # 读取 clickhouse中的 行情数据落到本地 tb_name
    query = ...
    return bs.read_ck(query, db_conf="databases.ck")

import blazestore.updater
# write into local file: bs.DB_PATH/tb_name
tb_name = "mc/stock_kline_day"
blazestore.updater.submit(tb_name=tb_name, 
                          fetch_fn=update_stock_kline_day, 
                          mode="auto", 
                          beg_date="2018-01-01", )
```

#### 2.customize data
```python
from blazestore import Factor

# 日频因子
def my_day_factor(date):
    """实现当天的因子计算逻辑"""
    ...
fac_myday = Factor(fn=my_day_factor)

# 分钟频因子, 增加形参 `end_time`
def my_minute_factor(date, end_time):
    """实现在end_time时的因子计算逻辑"""
    ...

fac_myminute = Factor(fn=my_minute_factor)
```

#### 3.expression database
```python
import blazestore as bs

# create expression database from polars dataframe
df_pl = bs.sql(query="select * from maket_data/kline_minute where date='2025-05-06';")
db = bs.from_polars(df_pl)

exprs = [
    "ind_pct(close, 1) as roc_intraday", 
    "ind_mean(roc_intraday, 20) as roc_ma20", 
]

result = db.sql(*exprs)
```