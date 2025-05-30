# BlazeStore 
🚀 The Blazing-Fast Data Toolkit for Quantitative Workflows
---
### Overview

**BlazeStore** is a high-performance, local-first data management toolkit tailored for quantitative research and financial data workflows. Built on top of Polars (Rust-powered DataFrame library), it enables fast in-memory computation, efficient disk I/O, and scalable partitioned storage, making it ideal for managing **large-scale datasets** such as tick, minute, and daily bar data.

It supports:

- Efficient local read/write
- Partitioned columnar storage using Parquet
- Integration with databases like MySQL and ClickHouse
- Built-in task scheduling and batch updating via DataUpdater
- Advanced factor engineering, including reusable, composable factor definitions

-----

### Key Features

| Feature                 | Description                                                  |
| ----------------------- | ------------------------------------------------------------ |
| 🔥 High Performance      | Leverages Polars (Rust-based) for memory efficiency, multi-core utilization, and TB-scale analysis |
| 📁 Partitioned Storage   | Supports automatic partitioning by date or other fields using Parquet format |
| 💾 Local Data Management | Efficiently store, query, and manage structured quantitative data locally |
| 🧮 SQL Interface         | Execute SQL-like queries directly on local data              |
| 🔄 Task Scheduling       | Schedule and execute batch data updates with the built-in DataUpdater |
| 🧬 Factor Engineering    | Define, compose, and compute complex factors efficiently     |

------

### Installation

Install from PyPI:

```bash
pip install -U blazestore
```

### Quick Start
```python
import blazestore as bs

# Get current settings
bs.get_settings()

# Assume you have a polars.DataFrame df containing minute-level K-line data
kline_df = ...  # Columns: date | time | asset | open | high | low | close | volume

# Persist to disk under table name "market_data/kline_minute", partitioned by date
tb_name = "market_data/kline_minute"
bs.put(kline_df, tb_name=tb_name, partitions=["date"])
print((bs.DB_PATH / tb_name).exists())  # True

# Query local data using SQL syntax
query = f"SELECT * FROM {tb_name} WHERE date = '2025-05-06';"
read_df = bs.sql(query)
```

-----

### Examples

#### 1.update data

```python
import blazestore as bs

# Implement update function
def update_stock_kline_day(tb_name, date):
    # fetch data from ClickHouse or other sources
    query = ...
    return bs.read_ck(query, db_conf="databases.ck")

# submit update task
import blazestore.updater
tb_name = "mc/stock_kline_day"
blazestore.updater.submit(tb_name=tb_name, 
                          fetch_fn=update_stock_kline_day, 
                          mode="auto", 
                          beg_date="2018-01-01", )
blazestore.updater.do(debug_mode=True)
```

#### 2.Custom Factor Definitions
```python
from blazestore import Factor

# Daily factor
def my_day_factor(date):
    """Factor logic for daily frequency"""
    ...
fac_myday = Factor(fn=my_day_factor)

# Minute-level factor with additional argument `end_time`
def my_minute_factor(date, end_time):
    """Factor logic at a specific end time"""
    ...

fac_myminute = Factor(fn=my_minute_factor)
```

#### 3. Expression Database
```python
import blazestore as bs

# Create expression database from polars dataframe
df_pl = bs.sql(query="select * from maket_data/kline_minute where date='2025-05-06';")
db = bs.from_polars(df_pl)
# Define and evaluate expressions
exprs = [
    "ind_pct(close, 1) as roc_intraday", 
    "ind_mean(roc_intraday, 20) as roc_ma20", 
]

result = db.sql(*exprs)
```

-----

### Use Cases

| Use Case                     | Description                                              |
| ---------------------------- | -------------------------------------------------------- |
| ✅ Market Data Pipeline       | Store and update historical tick, minute, and daily bars |
| ✅ Factor Library             | Build and maintain complex factor libraries              |
| ✅ Backtesting Framework      | Fast, reproducible backtest execution                    |
| ✅ Research Acceleration      | Perform efficient exploratory data analysis (EDA)        |
| ✅ Multi-strategy Development | Manage dependencies and shared logic across strategies   |
| ✅ Integration Layer          | Bridge between external databases and local analysis     |

----

### Why BlazeStore?

| Compared To    | BlazeStore Advantages                               |
| -------------- | --------------------------------------------------- |
| Pandas         | Faster performance, better memory handling          |
| CSV/JSON       | Build and maintain complex factor libraries         |
| Manual IO      | Fast, reproducible backtest execution               |
| Full DBMS      | Lightweight, no server setup, portable              |
| DVC/Delta Lake | Simpler, faster setup, optimized for quant research |

