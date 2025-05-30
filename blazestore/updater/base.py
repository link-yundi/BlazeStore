# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/29 13:04
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
from datetime import timedelta, datetime
from glob import glob
import os
import xcals
import ygo
from pathlib import Path
from .. import tb_path
from ..database import put, DB_PATH


class Table:

    def __init__(self,
                 name: str,
                 fetch_fn: callable,
                 description: str = "",
                 update_time: str = "16:30",):
        """
        数据源，配合 updater 进行多种场景的更新
        Parameters
        ----------
        name: str
            数据源名称，对应表名，比如 行情数据.分钟kline: mc/stock_kline_minute, 该数据源下的数据都会保存在 blazestore.DB_PATH/mc/stock_kline_minute 下
        fetch_fn: callable
            拉取数据的函数, 需要返回polars.DataFrame
        update_time: str
            更新时间
        description: 数据源的描述
        """
        self.name = name
        self.fetch_fn = ygo.delay(fetch_fn)(tb_name=name)
        self.description = description
        self._update_time = update_time
        self._last_run_file = DB_PATH / self.name / ".last_run"
        present = datetime.now().today()
        self._last_run_file.parent.mkdir(parents=True, exist_ok=True)

        if present.strftime("%H:%M") >= self._update_time:
            self.last_date = present.strftime(xcals.DATE_FORMAT)
        else:
            self.last_date = (present - timedelta(days=1)).strftime(xcals.DATE_FORMAT)

        self._working_num_ = 0

    @property
    def finished(self):
        return self._working_num_ <= 0

    def __add__(self, other):
        self._working_num_ += other
        return self

    def __sub__(self, other):
        self._working_num_ -= other
        return self

    @property
    def last_update_date(self):
        return self._read_last_run_date()

    def _read_last_run_date(self):
        if self._last_run_file.exists():
            with open(self._last_run_file, "r") as f:
                return f.read().strip()
        return

    def _write_last_run_date(self, date_str: str):
        with open(self._last_run_file, "w") as f:
            f.write(date_str)

    def get_existing_dates(self, safe: bool = False) -> set:
        """
        获取本地已存在的日期列表
        Parameters
        ----------
        safe: bool
            是否使用安全模式, 安全模式不仅仅通过文件夹date=*来判定某个日期是否存在数据，而是通过文件夹内是否存在数据来判定
        Returns
        -------

        """
        tbpath = tb_path(self.name)
        partition_dirs = glob(str(tbpath / "date=*"))
        if not safe:
            return set(os.path.relpath(p, tbpath).split("=")[-1] for p in partition_dirs)
        else:
            partition_dirs = glob(str(tbpath / "date=*" / "*.parquet"))
            return set(os.path.relpath(p, tbpath).split("=")[-1].split("/")[0] for p in partition_dirs if Path(p).stat().st_size > 0)

    def need_update(self,
                    mode: str = "auto",
                    beg_date: str = None,
                    end_date: str = None,
                    force: bool = False,
                    safe: bool = False) -> bool:
        """
        判断是否需要更新数据

        Parameters
        ----------
        mode : str
            更新模式 ("auto" 或 "full")
        beg_date : str | None
            请求更新的起始日期
        end_date : str | None
            请求更新的结束日期
        force : bool
            是否强制更新
        safe: bool
            是否启用安全模式, 安全模式不仅仅通过文件夹date=*来判定某个日期是否存在数据，而是通过文件夹内是否存在数据来判定

        Returns
        -------
        bool

        """
        if force:
            return True
        existing_dates = self.get_existing_dates(safe=safe)
        if mode.lower() == "auto" and not beg_date:
            # 自动补齐模式：只检查最后一天是否已经更新
            local_last_date = self._read_last_run_date()
            return (not local_last_date) or (local_last_date < self.last_date)
        elif mode.lower() == "full" or beg_date is not None:
            # 全量更新或指定了 beg_date：检查日期范围内的数据是否完整
            date_list = xcals.get_tradingdays(beg_date, end_date)
            missing_dates = set(date_list).difference(existing_dates)
            return len(missing_dates) > 0  # 有缺失就更新
        return False

    def save(self, df, partitions: list[str] | None = None, ):
        """
        将 数据 保存到本地
        Parameters
        ----------
        df: polars.DataFrame
        partitions: list[str]| None: 分区字段
        Returns
        -------

        """
        put(df, tb_name=self.name, partitions=partitions)

    def update_daily(self, date: str):
        """每日更新逻辑"""
        df = self.fetch_fn(date=date)
        if df is not None:
            self.save(df, partitions=["date"])

    def update_once(self):
        """一次性更新"""
        df = self.fetch_fn()
        if df is not None:
            self.save(df, partitions=None)

    def close(self):
        """更新完成"""
        self._write_last_run_date(self.last_date)
