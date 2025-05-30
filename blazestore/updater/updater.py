# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/23 01:34
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import xcals
import ygo
import ylog

from .base import Table
from .. import database


class UpdateMode:
    FULL = "full"  # 全量更新
    AUTO = "auto"  # 自动补齐缺失日期


class DataUpdater:
    """
    数据更新器
    路径：{blazestore.DB_PATH}/provider/{name}
    """

    def __init__(self, name: str, ):
        """
        数据更新器
        :param name: 数据更新器名称
        """
        self.name = name
        self._tasks = list()
        self.logger = ylog.get_logger(self.name, )
        self._overwrite = False
        self._working_tb = dict()  # dict[tb.name, tb]
        self._debug_mode = False

    def add_table(self,
                  table: Table,
                  mode: UpdateMode = UpdateMode.AUTO,
                  beg_date: str = "",
                  end_date: str = ""):
        """
        添加数据源表格
        Parameters
        ----------
        table: Table
        mode: str
            - full: 全量更新
            - auto: 自动补齐缺失日期
        beg_date: str
        end_date: str
        Returns
        -------

        """
        end_date = xcals.today() if not end_date else end_date
        signature_params = ygo.fn_signature_params(table.fetch_fn)
        if "date" not in signature_params:
            return self._add_task(task_name=table.name, update_fn=table.update_once)
        if mode == UpdateMode.FULL:
            # 全量更新
            if not beg_date:
                self.logger.warning(f"[{table.name}] 数据表(mode={UpdateMode.FULL})未指定`beg_date`参数")
                return
            dateList = xcals.get_tradingdays(beg_date, end_date)
            for date in dateList:
                self._add_task(task_name=table.name, update_fn=ygo.delay(table.update_daily)(date=date))
            return
        elif mode == UpdateMode.AUTO:
            # 自动补齐缺失日期
            if not database.has(table.name) or self._overwrite:
                # 表单不存在，使用全量更新模式
                return self.add_table(table, mode=UpdateMode.FULL, beg_date=beg_date, end_date=end_date, )
            else:
                existed_dateList = table.get_existing_dates(safe=True)
                # 获取缺失的日期列表
                dateList = xcals.get_tradingdays(beg_date, end_date)
                missing_dateList = set(dateList).difference(existed_dateList)
                missing_dateList = sorted(list(missing_dateList))
                for date in missing_dateList:
                    self._add_task(task_name=table.name, update_fn=ygo.delay(table.update_daily)(date=date))
        else:
            self.logger.error(
                f"[{table.name}] 不支持的更新模式：{mode}。支持的更新模式为: {UpdateMode.FULL}, {UpdateMode.AUTO}")
            return

    def submit(self,
               tb_name: str,
               fetch_fn: callable,
               mode="auto",
               update_time: str = "16:30",
               beg_date: str = "",
               end_date: str = "",
               force: bool = False,):
        """
        提交拉取数据任务
        Parameters
        ----------
        tb_name: str
            表格名（路径）
        fetch_fn: callable
            拉取数据的具体实现
        mode: str
            auto / full, 默认auto
        update_time: str
            更新时间
        beg_date: str
        end_date: str
        force: bool
            是否强制更新
        Returns
        -------

        Notes
        -------
            - beg_date 赋值:
                - mode = 'auto': 检查 beg_date->end_date 数据完整性
                - mode = 'full': 检查 beg_date->end_date 数据完整性
            - beg_date 不赋值:
                - mode = 'auto': 只检查最后一天是否更新, 如果最后一天已经更新, 则跳过更新任务
                - mode = 'full': 报错，必填`beg_date`
        """
        end_date = xcals.today() if not end_date else end_date
        tb = Table(name=tb_name, fetch_fn=fetch_fn, update_time=update_time, )
        if tb.need_update(mode=mode,
                          beg_date=beg_date,
                          end_date=end_date,
                          force=force,
                          safe=True):
            self.add_table(tb, mode=mode, beg_date=beg_date, end_date=end_date)
            if tb.name in self._working_tb:
                self.logger.warning(f"[{tb.name}] 重复，请勿重复提交任务或者更换表名")
            else:
                if tb.name not in self._working_tb:
                    self._working_tb[tb.name] = tb
                self._working_tb[tb.name] += 1
        else:
            self.logger.info(f"[{tb.name}] 数据完整性检查通过")

    def wrap_fn(self, task_name: str, update_fn: callable):
        """包装函数，添加异常处理"""
        try:
            update_fn()
            failed = False
            return task_name, failed
        except Exception as e:
            exc_info = e if self._debug_mode else None
            self.logger.error(ygo.FailTaskError(task_name=task_name, error=e), exc_info=exc_info)
            failed = True
            return task_name, failed

    def _add_task(self, task_name: str, update_fn: callable):
        """添加任务"""
        self._tasks.append((task_name, ygo.delay(self.wrap_fn)(task_name=task_name, update_fn=update_fn)))

    def do(self,
           n_jobs: int = 10,
           backend: str = "threading",
           debug_mode: bool = False):
        """
        执行任务
        :param n_jobs: 并发数
        :param backend: loky/threading/multiprocessing
        :param debug_mode:
        :return:
        """
        self._debug_mode = debug_mode
        self.logger.info(f"[{self.name}] 更新数据")
        failed_num = 0
        with ygo.pool(n_jobs=n_jobs, backend=backend) as go:
            for task_name, task in self._tasks:
                go.submit(task, job_name=task_name)()
            for task_name, if_failed in go.do():
                if if_failed:
                    failed_num += if_failed
                self._working_tb[task_name] += ~if_failed
                if self._working_tb[task_name].finished:
                    self._working_tb[task_name].close()
        if failed_num < 1:
            for task_name, tb in self._working_tb.items():
                if tb.finished:
                    self.logger.info(f"[{tb.name}] 更新成功，最新数据日期：{tb.last_date}")
        self.logger.info(f"[{self.name}] 更新完成，失败任务数：{failed_num:02d}/{len(self._tasks):02d}")


_default_updater = DataUpdater(name="BlazeStoreUpdater")
submit = _default_updater.submit
do = _default_updater.do
