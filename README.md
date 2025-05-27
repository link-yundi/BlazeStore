# BlazeStore 
🚀 blazestore —— The blazing-fast data toolkit for quantitative workflows
qdb 专注于本地量化数据的高效管理与读写，具备以下特点：
- 持久化: 基于polars的高性能读写
- 便捷性: 内存数据库-根据polars读取parquet分区文件，支持sql查询以及构造表达式数据库
- 时效性: 提供数据更新器，用于每日更新
- 扩展性: 对于自建数据源，通过构造Factor来计算、读写

### 安装
```bash
pip install -U blazestore
```

### 快速开始
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

# 读取
query = f"select * from {tb_name} where date = '2025-05-06';"

read_df = bs.sql(query)
```

### 示例
#### 1.数据更新
```python
import blazestore as bs
from blazestore import DataUpdater

# 数据更新的具体实现
def update_kline_daily():
    # 读取 clickhouse中的 行情数据落到本地
    query = ...
    kline_minute = bs.read_ck(query, db_conf="databases.ck")
    bs.put(kline_minute, tb_name="market_data/kline_minute", partitions=["date", ])

# 创建更新器 
updater = DataUpdater(name="行情数据更新器")
updater.add_task(task_name="分钟行情", update_fn=update_kline_daily)
updater.do()
```

#### 2.自定义因子
```python
from blazestore import Factor

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
