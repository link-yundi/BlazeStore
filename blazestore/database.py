# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2024/7/1 09:44
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import re
import urllib.parse
from pathlib import Path

import clickhouse_df
import polars as pl
from dynaconf import Dynaconf

import ylog
from .parse import extract_table_names_from_sql

USERHOME = Path("~").expanduser() # 用户家目录
NAME = "BlazeStore"
DB_PATH = USERHOME / NAME
CONFIG_PATH = DB_PATH / "conf" / "settings.toml"
if not CONFIG_PATH.exists():
    try:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        ylog.error(f"[{NAME}]配置文件生成失败: {e}")
    template_content = f"""[paths]
{NAME}="{DB_PATH}" 

# Configuration of database
[databases]
# [databases.ck]
# urls=["<host1>:<port1>", "<host2>:<port2>",]
# user="xxx"
# password="xxxxxx"
# [databases.mysql]
# url="<host>:<port>"
# user="xxxx"
# password="xxxxxx"

# 视情况自由增加其他配置\n"""
    with open(CONFIG_PATH, "w") as f:
        f.write(template_content)
    ylog.info(f"[{NAME}]生成配置文件: {CONFIG_PATH}")

def get_settings():
    try:
        return Dynaconf(settings_files=[CONFIG_PATH])
    except Exception as e:
        ylog.error(f"[{NAME}]读取配置文件失败: {e}")
        return {}

# 读取配置文件覆盖
_settiings = get_settings()
if _settiings is not None:
    DB_PATH = _settiings.get(f"paths.{NAME}", "")
    DB_PATH = Path(DB_PATH)


# ======================== 本地数据库 catdb ========================
def tb_path(tb_name: str) -> Path:
    """
    返回指定表名 完整的本地路径
    Parameters
    ----------
    tb_name: str
       表名，路径写法: a/b/c
    Returns
    -------
    pathlib.Path
        full_abs_path: pathlib.Path
        完整的本地绝对路径 $DB_PATH/a/b/c
    """
    return Path(DB_PATH, tb_name)


def put(df: pl.DataFrame, tb_name: str, partitions: list[str] | None = None, abs_path: bool = False):
    """
    将一个DataFrame写入到指定名称的表格目录中，支持分区存储。

    该函数负责将给定的DataFrame（df）根据提供的表名（tb_name）写入到本地文件系统中。
    如果指定了分区（partitions），则会按照这些分区列将数据分割存储。此外，可以通过abs_path参数
    指定tb_name是否为绝对路径。如果目录不存在，会自动创建目录。

    Parameters
    ----------
    df: polars.DataFrame
    tb_name: str
        表的名称，用于确定存储数据的目录
    partitions: list[str] | None
        指定用于分区的列名列表。如果未提供，则不进行分区。
    abs_path: bool
        tb_name是否应被视为绝对路径。默认为False。

    Returns
    -------

    """
    if not abs_path:
        tbpath = tb_path(tb_name)
    else:
        tbpath = Path(tb_name)
    if not tbpath.exists():
        tbpath.mkdir(parents=True, exist_ok=True)
    if partitions is not None:
        df.write_parquet(tbpath, partition_by=partitions)
    else:
        df.write_parquet(tbpath / "data.parquet")

def has(tb_name: str) -> bool:
    """
    判定给定的表名是否存在
    Parameters
    ----------
    tb_name: str

    Returns
    -------

    """
    return tb_path(tb_name).exists()

def sql(query: str, abs_path: bool = False, lazy: bool = True):
    """
    sql 查询，从本地paquet文件中查询数据

    Parameters
    ----------
    query: str
        sql查询语句
    abs_path: bool
        是否使用绝对路径作为表路径。默认为False
    lazy: bool
        惰性模式
    Returns
    -------

    """
    tbs = extract_table_names_from_sql(query)
    convertor = dict()
    for tb in tbs:
        if not abs_path:
            db_path = tb_path(tb)
        else:
            db_path = tb
        format_tb = f"read_parquet('{db_path}/**/*.parquet')"
        convertor[tb] = format_tb
    pattern = re.compile("|".join(re.escape(k) for k in convertor.keys()))
    new_query = pattern.sub(lambda m: convertor[m.group(0)], query)
    if not lazy:
        return pl.sql(new_query).collect()
    return pl.sql(new_query)


def read_mysql(query: str, db_conf: str = "databases.mysql") -> pl.DataFrame:
    """
    从MySQL数据库中读取数据。
    Parameters
    ----------
    query: str
        查询语句
    db_conf: str
        对应的配置 $DB_PATH/conf/settings.toml
    Returns
    -------
    """
    try:
        db_setting = get_settings().get(db_conf, {})
        required_keys = ['user', 'password', 'url']
        missing_keys = [key for key in required_keys if key not in db_setting]
        if missing_keys:
            raise KeyError(f"Missing required keys in database config: {missing_keys}")

        user = urllib.parse.quote_plus(db_setting['user'])
        password = urllib.parse.quote_plus(db_setting['password'])
        uri = f"mysql://{user}:{password}@{db_setting['url']}"
        return pl.read_database_uri(query, uri)

    except KeyError as e:
        raise RuntimeError("Database configuration error: missing required fields.") from e
    except Exception as e:
        raise RuntimeError(f"Failed to execute MySQL query: {e}") from e


def read_ck(query: str, db_conf: str = "databases.ck") -> pl.DataFrame:
    """
    从Clickhouse集群读取数据。
    Parameters
    ----------
    query: str
        查询语句
    db_conf: str
        对应的配置 $DB_PATH/conf/settings.toml
    Returns
    -------
    """
    try:
        db_setting = get_settings().get(db_conf, {})
        required_keys = ['user', 'password', 'urls']
        missing_keys = [key for key in required_keys if key not in db_setting]
        if missing_keys:
            raise KeyError(f"Missing required keys in database config: {missing_keys}")

        user = urllib.parse.quote_plus(db_setting['user'])
        password = urllib.parse.quote_plus(db_setting['password'])

        with clickhouse_df.connect(db_setting['urls'], user=user, password=password):
            return clickhouse_df.to_polars(query)

    except KeyError as e:
        raise RuntimeError("Database configuration error: missing required fields.") from e
    except Exception as e:
        raise RuntimeError(f"Failed to execute ClickHouse query: {e}") from e
