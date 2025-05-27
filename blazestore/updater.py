# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/23 01:34
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import os
from datetime import datetime, timedelta
import ygo
import ylog
from .database import DB_PATH

DATE_FORMAT = "%Y-%m-%d"
NAME_UPDATER = "data_updater"
TB_UPDATER = DB_PATH/NAME_UPDATER

class DataUpdater:
    """
    数据更新器
    路径：{blazestore.DB_PATH}/provider/{name}
    """

    def __init__(self, name: str, update_time="16:30"):
        """
        数据更新器
        :param name: 数据更新器名称
        :param update_time: 数据更新时间，默认16:30
        """
        self.name = name
        self._tb_path = DB_PATH / name
        os.makedirs(self._tb_path, exist_ok=True)
        self._update_time = update_time
        self.present = datetime.now().today()

        if self.present.strftime("%H:%M") >= self._update_time:
            self.last_date = self.present.strftime(DATE_FORMAT)
        else:
            self.last_date = (self.present - timedelta(days=1)).strftime(DATE_FORMAT)

        self._tasks = list()
        self._last_run_file = self._tb_path / ".last_run"
        self.logger = ylog.get_logger(NAME_UPDATER)

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

    def wrap_fn(self, task_name: str, update_fn: callable):
        """包装函数，添加异常处理"""
        try:
            update_fn()
            return 0
        except Exception as e:
            self.logger.error(ygo.FailTaskError(task_name=task_name, error=e))
            return 1

    def add_task(self, task_name: str, update_fn: callable):
        """添加任务"""
        self._tasks.append((task_name, ygo.delay(self.wrap_fn)(task_name=task_name, update_fn=update_fn)))

    def do(self,
           overwrite: bool = False,
           n_jobs: int = 10,
           backend: str = "threading"):
        """
        执行任务
        :param overwrite: 是否覆盖现有数据
        :param n_jobs: 并发数
        :param backend: loky/threading/multiprocessing
        :return:
        """
        if not overwrite:
            local_last_date = self._read_last_run_date()
            if local_last_date is not None:
                if local_last_date >= self.last_date:
                    self.logger.info(f"[{self.name}] 已是最新数据，跳过更新")
                    return
        self.logger.info(f"[{self.name}] 更新数据")
        failed_num = 0
        with ygo.pool(n_jobs=n_jobs, backend=backend) as go:
            for task_name, task in self._tasks:
                go.submit(task, job_name=task_name)()
            for status in go.do():
                failed_num += status
        if failed_num < 1:
            self._write_last_run_date(self.last_date)
            self.logger.info(f"[{self.name}] 更新成功，最新数据日期：{self.last_date}")
        self.logger.info(f"[{self.name}] 更新完成，失败任务数：{failed_num:02d}/{len(self._tasks):02d}")
