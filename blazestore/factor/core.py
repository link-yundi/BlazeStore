# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/26 08:47
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

from __future__ import annotations

import datetime
import hashlib
import inspect as inspect_
import itertools
import os
from pathlib import Path

import pandas as pd
import polars as pl
import xcals
from rich import inspect
from varname import varname

import ygo
from .errors import FactorGetError
from .. import database

FIELD_DATE = "date"
FIELD_TIME = "time"
FIELD_ASSET = "asset"
FIELD_VERSION = "version"
FIELD_ENDTIME = "end_time"
TYPE_FIXEDTIME = 'fixed_time'  # 因子插入时间是固定的
TYPE_REALTIME = "real_time"  # 因子插入时间是实时的
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"

INDEX = (FIELD_DATE, FIELD_TIME, FIELD_ASSET,)


def _get_value_firsttime(fac: Factor, date: str) -> pl.DataFrame:
    """
    第一次落数据: 当本地没有数据或者数据都为空


    Parameters
    ----------
    fac : Factor
        因子对象，包含因子计算函数和其他相关信息。
    date : str
        日期，用于指定因子计算的日期，格式为 yyyy-mm-dd

    Returns
    -------
    data : pl.DataFrame | None
        处理后的因子计算结果数据

    Raises
    ------
    Exception
        如果因子计算函数返回的数据为空或数据类型不符合要求，则抛出异常。
    Exception
        如果因子计算结果中缺少必要的 `asset` 列，则抛出异常。
    """
    data = ygo.delay(fac.fn)(this=fac, date=date)()
    if data is None:
        return data
    if not (isinstance(data, (pl.DataFrame, pd.Series, pd.DataFrame))):
        raise Exception("因子计算函数需要返回 polars.DataFrame | pandas.Series | pandas.DataFrame")
    if isinstance(data, (pd.DataFrame, pd.Series)):
        index_levs = data.index.nlevels
        if index_levs == 1:
            assert FIELD_ASSET == data.index.name, "因子计算结果index中必须包含`{FIELD_ASSET}`"
        else:
            assert FIELD_ASSET in data.index.names, "因子计算结果index中必须包含`{FIELD_ASSET}`"
        data = pl.from_pandas(data.reset_index())
    if FIELD_ASSET not in data.columns:
        if data.is_empty():
            raise Exception(f"Empty Value!")
        raise Exception("因子计算函数返回值中必须包含`{FIELD_ASSET}`列")
    index = [FIELD_DATE, FIELD_TIME, FIELD_ASSET]
    val_fields = data.drop(index, strict=False).columns

    data = data.unique().fill_nan(None)
    if data.drop_nulls().is_empty():
        raise Exception(f"Empty Value!")
    if FIELD_DATE not in data.columns:
        data = data.with_columns(pl.lit(date).alias(FIELD_DATE))
    if FIELD_TIME not in data.columns:
        data = data.with_columns(pl.lit(fac.end_time).alias(FIELD_TIME))

    data = data.select(*index, *val_fields, )

    database.put(data, tb_name=fac.tb_name, partitions=[FIELD_DATE, ])

    return data.sort(index)


def get_value(fac: Factor,
              date: str,
              codes: list[str] | None = None,
              time: str = '15:00:00',
              avoid_future: bool = True,
              rt: bool = True) -> pl.DataFrame | None:
    """
    获取指定日期和时间的最新数据。

    Parameters
    ----------
    fac : Factor
        因子对象，包含因子的相关信息。
    date : str
        日期字符串，格式为 yyyy-mm-dd。
    codes : Iterable[str]
        证券代码列表，可选，默认为 None。
    time : str
        时间字符串，默认为 '15:00:00'。
    avoid_future: bool
        是否避免未来数据，默认 True
        - True: 当取值 time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
        - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
    rt: bool
        是否实时取值，默认 True
        - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
        - False: fac保持原始设置, end_time保持不变
    Returns
    -------
    polars.DataFrame | None
        包含指定日期和时间的最新数据的 DataFrame。
        如果只有一列数据，则返回 pandas.Series
    """
    if isinstance(date, (datetime.date, datetime.datetime)):
        date = date.strftime(DATE_FORMAT)
    if rt:
        fac = fac(end_time=time)
    val_date = xcals.get_recent_tradeday(date)  # 数据日期
    # 如果avoid_future 为 True, 且查询时间早于因子的结束时间，使用上一个交易日的数据
    if avoid_future and time < fac.insert_time:
        val_date = xcals.shift_tradeday(val_date, -1)
    if not database.has(Path(fac.tb_name) / f"date={val_date}"):
        data = _get_value_firsttime(fac=fac, date=val_date)
    else:
        data = database.sql(f"select * from {fac.tb_name} where date='{val_date}';", lazy=True).drop(
            FIELD_VERSION).collect()
    if codes is None:
        return data
    cols = data.columns
    codes = pl.DataFrame({FIELD_ASSET: codes})
    return data.join(codes, on=FIELD_ASSET, how='inner')[cols]


def get_value_depends(depends: list[Factor],
                      date: str,
                      codes: list[str] | None = None,
                      time: str = '15:00:00',
                      avoid_future: bool = True,
                      rt: bool = True) -> pl.DataFrame:
    """
    获取依赖因子的值，并合并成一张宽表。

    Parameters
    ----------
    depends : list[Factor] | None
        可选的因子列表，表示依赖的因子。
    date : str
        日期字符串，用于获取因子值的日期, 格式为'yyyy-mm-dd'。
    codes : Iterable[str]
        可选的证券代码列表，默认为 None。
    time : str
        时间字符串，默认为 '15:00:00'。
    avoid_future: bool
        是否避免未来数据，默认 True
        - True: 当取值time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
        - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
    rt: bool
        是否实时取值，默认 True
        - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
        - False: fac保持原始设置, end_time保持不变

    Returns
    -------
    polars.DataFrame | None

    Notes
    -----
    - 如果 `depends` 为 None 或空列表，则函数直接返回 None。
    - 函数会为每个因子获取其值，并将这些值合并成一个宽表。
    - 如果某个因子的值为 None，则跳过该因子。
    - 存在多列的因子，列名会被重命名，便于阅读与避免冲突, 命名规则为 {fac.name}.<columns>。
    """
    if depends is None:
        return
    if len(depends) == 0:
        return
    depend_vals = list()
    for depend in depends:
        val = get_value(fac=depend,
                        date=date,
                        codes=codes,
                        time=time,
                        avoid_future=avoid_future,
                        rt=rt, )
        # 重命名columns
        if val is None:
            continue
        columns = val.columns
        if len(columns) > 4:
            new_columns = [
                col_name if col_name in [FIELD_DATE, FIELD_TIME, FIELD_ASSET] else f'{depend.name}.{col_name}' for
                col_name in
                columns]
            val.columns = new_columns
        depend_vals.append(val)
    return pd.concat(depend_vals, axis=1)


def _check_missing_date_(fac: Factor, beg_date, end_date, ):
    """检验数据缺失的日期"""
    dateList = xcals.get_tradingdays(beg_date=beg_date, end_date=end_date)
    if not database.has(fac.tb_name):
        return dateList
    # 查询本地有数据的日期
    fac_path = database.tb_path(fac.tb_name)
    schema = pl.scan_parquet(fac_path).collect_schema()
    columns = schema.names()
    cond = " OR ".join([f"'{col}' IS NOT NULL" for col in columns])
    sql = f"""SELECT date
                FROM {fac.tb_name}
            WHERE date BETWEEN '{beg_date}' AND '{end_date}'
                AND ({cond})
            GROUP BY date 
            HAVING count() > 0;"""
    exist_dateList = database.sql(sql, lazy=False)["date"].cast(pl.Utf8).to_list()
    return sorted(list(set(dateList) - set(exist_dateList)))


def _generate_complete_tasks(*facs: Factor, beg_date: str, end_date: str, times: list[str], ):
    """生成补齐数据的任务"""
    # 补齐数据
    beg_date = xcals.shift_tradeday(beg_date, -1)
    for fac in facs:
        for time in times:
            fac = fac(end_time=time)
            for loss_date in _check_missing_date_(fac, beg_date, end_date):
                job = ygo.delay(get_value)(fac=fac,
                                           date=loss_date,
                                           time=time,
                                           with_date=False,
                                           rt=True,
                                           avoid_future=True)
                job.job_name = f"{time} {fac.name} completing"
                yield job


def get_history(fac: Factor,
                beg_date: str,
                end_date: str,
                codes: list[str] | None = None,
                time='15:00:00',
                avoid_future: bool = True,
                rt: bool = True,
                show_progress: bool = True,
                n_jobs: int = 7, ):
    """
    获取指定日期范围内的因子值。

    Parameters
    ----------
    fac : Factor
        因子对象，表示要查询的因子。
    beg_date : str
        开始日期, 格式 'yyyy-mm-dd'。
    end_date : str
        结束日期, 格式 'yyyy-mm-dd'。
    codes : list[str] | None
        可选的证券代码列表，默认为None。
    time : str
        时间，默认为'15:00:00', 格式 hh:mm:ss
    n_jobs : int
        补齐数据的并发任务数，默认为7
    avoid_future: bool
        是否避免未来数据，默认 True
        - True: 当取值time < fac.insert_time，取不到当天的数据，只能取上一个交易日的数据
        - False: 当取值 time < fac.insert_time时, 可以取到当天的数据
    rt: bool
        是否实时取值，默认 True
        - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
        - False: fac保持原始设置, end_time保持不变
    show_progress: bool
        是否显示进度，默认True

    Returns
    -------
    polars.DataFrame | None

    Notes
    -----
    - 如果指定的时间早于因子的结束时间，且 avoid_future 为 True，则开始和结束日期都向前移动一个交易日。
    - 如果数据不完整，会自动补齐缺失的数据。
    - 最终结果会按日期和证券代码排序。
    """

    beg_date = xcals.get_recent_tradeday(beg_date)
    # 数据补完后读取
    fac = fac(end_time=time) if rt else fac
    miss_dateList = _check_missing_date_(fac=fac, beg_date=xcals.shift_tradeday(beg_date, -1), end_date=end_date)
    if len(miss_dateList) > 0:
        with ygo.pool(show_progress=show_progress, n_jobs=n_jobs, ) as go:
            for miss_date in miss_dateList:
                go.submit(get_value, f"{time} {fac.name} completing")(fac=fac,
                                                                      date=miss_date,
                                                                      time=time,
                                                                      with_date=False,
                                                                      rt=True,
                                                                      avoid_future=True)
            go.do()

    query_sql = f"""
        SELECT *
        FROM {fac.tb_name}
        WHERE date BETWEEN '{beg_date}' AND '{end_date}';
        """
    result = database.sql(query_sql, lazy=False).drop(FIELD_VERSION).with_columns(pl.col(FIELD_DATE).cast(pl.Utf8))
    cols = result.columns
    if avoid_future and time < fac.insert_time:
        dateList = xcals.get_tradingdays(beg_date, end_date)
        next_dateList = xcals.get_tradingdays(xcals.shift_tradeday(beg_date, 1), xcals.shift_tradeday(end_date, 1))
        shift_date_map = {old_date: next_dateList[i] for i, old_date in enumerate(dateList)}
        result = result.group_by(FIELD_DATE).map_groups(
            lambda df: df.with_columns(pl.lit(shift_date_map[df[FIELD_DATE][0]]).alias(FIELD_DATE)))
    if result is not None and codes is not None:
        target_index = pl.DataFrame({FIELD_ASSET: codes})
        result = target_index.join(result, on=FIELD_ASSET, how="left")
    if codes is not None:
        result = result.join(pl.DataFrame({FIELD_ASSET: codes}), on=FIELD_ASSET, how="inner")
    # 调整列的顺序
    result = result.select(cols)
    return result.sort(INDEX)


def _generate_get_tasks(*facs: Factor,
                        beg_date: str,
                        end_date: str,
                        times: list[str],
                        codes: list[str] | None = None,
                        avoid_future: bool = True,
                        rt: bool = True,
                        ):
    """生成读数据的任务"""
    # beg_date = jydata.shift_tradeday(beg_date, -1)
    for fac in facs:
        for time in times:
            job = ygo.delay(get_history)(fac=fac,
                                         beg_date=beg_date,
                                         end_date=end_date,
                                         time=time,
                                         codes=codes,
                                         n_jobs=0,
                                         avoid_future=avoid_future,
                                         rt=rt, )
            job.job_name = f"{time} getting"
            yield job


def get_history_depends(depends: list[Factor],
                        beg_date: str,
                        end_date: str,
                        times: list[str],
                        codes: list[str] | None = None,
                        show_progress: bool = True,
                        avoid_future: bool = True,
                        rt: bool = True,
                        n_jobs=7, ):
    """
    获取依赖因子的指定日期期间的值

    Parameters
    ----------
    depends : list[Factor]
        需要获取历史值的因子列表。
    beg_date : str
        开始日期，格式为 'yyyy-mm-dd'。
    end_date : str
        结束日期，格式为 'yyyy-mm-dd'。
    codes : Iterable[str]
        股票代码列表，默认为 None。
    times : listl[str]
        取值时间序列，默认为 ['15:00:00'], 格式为 'hh:mm:ss'
    show_progress : bool
        是否显示进度条，默认为 True。
    n_jobs : int
        并行任务数，默认为 7。
    avoid_future: bool
        是否避免未来数据，默认 True
        - True: 当取值time < fac.insert_time，取不到当天的数据，只能取上一个交易日的数据
        - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
    rt: bool
        是否实时取值，默认 True
        - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
        - False: fac保持原始设置, end_time保持不变

    Returns
    -------
    polars.DataFrame | None

    Notes
    -----
    - 最终结果会按日期和股票代码排序。
    """

    if depends is None:
        return
    if len(depends) == 0:
        return
    depend_vals = dict()  # dict[str, polars.DataFrame]

    with ygo.pool(show_progress=show_progress, n_jobs=n_jobs, ) as go:
        # 补数据任务
        for job in _generate_complete_tasks(*depends, beg_date=beg_date, end_date=end_date, times=times):
            go.submit(job, job_name=job.job_name)()
        go.do()
        # 拿数据任务
        for job in _generate_get_tasks(*depends,
                                       beg_date=beg_date,
                                       end_date=end_date,
                                       times=times,
                                       codes=codes,
                                       avoid_future=avoid_future,
                                       rt=rt):
            go.submit(job, job_name=job.job_name)()

        for val, (depend, time) in zip(go.do(), itertools.product(depends, times)):
            if time not in depend_vals:
                depend_vals[time] = list()
            # 重命名columns
            if val is None:
                continue
            columns = val.columns
            if len(columns) == 4:
                new_columns = [
                    col_name if col_name in INDEX else f'{depend.name}' for
                    col_name in
                    columns]
                val.columns = new_columns
            elif len(columns) > 4:
                new_columns = [
                    col_name if col_name in INDEX else f'{depend.name}.{col_name}' for
                    col_name in
                    columns]
                val.columns = new_columns
            depend_vals[time].append(val)
    lazy_dfs = [pl.concat(vals, how="align").lazy() for vals in depend_vals.values()]
    big_df = pl.concat(lazy_dfs, how="vertical").collect()
    big_df = big_df.sort(by=INDEX)
    return big_df


def cache_history(*facs: Factor,
                  beg_date: str,
                  end_date: str,
                  times: list[str],
                  show_progress: bool = True,
                  avoid_future: bool = True,
                  rt: bool = True,
                  n_jobs=7, ):
    """
    缓存因子值, 只落到本地，不做其他处理

    Parameters
    ----------
    facs : Factor
        需要缓存历史值的因子
    beg_date : str
        开始日期，格式为 'yyyy-mm-dd'。
    end_date : str
        结束日期，格式为 'yyyy-mm-dd'。
    times : listl[str]
        取值时间序列，默认为 ['15:00:00'], 格式为 'hh:mm:ss'
    show_progress : bool
        是否显示进度条，默认为 True。
    n_jobs : int
        并行任务数，默认为 7。
    avoid_future: bool
        是否避免未来数据，默认 True
        - True: 当取值time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
        - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
    rt: bool
        是否实时取值，默认 True
        - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
        - False: fac保持原始设置, end_time保持不变

    """
    if not facs:
        return
    with ygo.pool(show_progress=show_progress, n_jobs=n_jobs, ) as go:
        # 补数据任务
        for job in _generate_complete_tasks(*facs, beg_date=beg_date, end_date=end_date, times=times):
            go.submit(job, job_name=job.job_name)()
        go.do()
        # 拿数据任务
        for job in _generate_get_tasks(*facs,
                                       beg_date=beg_date,
                                       end_date=end_date,
                                       times=times,
                                       codes=None,
                                       avoid_future=avoid_future,
                                       rt=rt):
            go.submit(job, job_name=job.job_name)()

        # 启动任务
        go.do()


class Factor:
    """
    因子类
    Examples
    --------

    日频因子

    >>> def fac1(date, ):
    ...     ...
    >>> fac_DayFac1 = Factor(fn=fac1)
    >>> fac_DayFac1.name
    DayFac1

    分钟频因子

    >>> def fac2(date, end_time):
    ...     ...
    >>> fac_MinuteFac = Factor(fn=fac2)
    >>> fac_MinuteFac(end_time="09:31:00").name
    MinuteFac

    多依赖因子

    >>> def fac3(this: Factor, date, ):
    ...     depend_big_df = this.get_history_depends(date, )
    ...     ...
    >>> fac_MultiFac = Factor(fac_DayFac1, fac_MinuteFac, fn=fac3)

    """

    def __init__(self, *depends: Factor, fn: callable, name: str = None, frame: int = 1, share_params: list = None):
        """
        初始化Factor类的实例。
        Parameters
        ----------
        *depends : Factor
            可变数量的Factor实例，表示当前函数依赖的因子。
        fn : callable
            可调用对象，每天的因子计算逻辑的具体实现
        name : str | None
            因子的名称，默认为None。如果未提供名称，将尝试从调用栈中获取。
        frame : int
            调用栈的层级，默认为1。
        share_params: list
            共享参数: 顶层的Factor的参数会传递到底层的依赖因子

        Notes
        -----
        - 如果提供了依赖因子，会根据依赖因子的版本号和当前因子的版本号重新计算版本号。
        - 如果没有提供名称，会尝试从调用栈中获取名称。
        - 根据因子计算逻辑函数中是否带有 `end_time` 形参来确定因子的类型(日频还是分钟频)
        """
        self._frame = frame
        self.fn = fn
        self.__doc__ = fn.__doc__
        self._fn_info = ygo.fn_info(fn)
        self.fn_params = ygo.fn_params(fn)
        self.version = hashlib.md5(self._fn_info.encode()).hexdigest()
        self._depends = [depend for depend in depends]
        if len(self._depends) > 0:
            if share_params is not None:
                depend_params = {k: v for k, v in self.fn_params if k in share_params}
                self._depends = [depend(**depend_params) for depend in depends]
            depends_version = [depend.version for depend in self._depends]
            depends_version.append(self.version)
            depends_version.sort()
            self.version = hashlib.md5(f"{','.join(depends_version)}".encode()).hexdigest()
        self._params = {k: v for k, v in self.fn_params}
        default_insettime = "15:00:00"
        self.end_time = self._params.get(FIELD_ENDTIME, default_insettime)
        self.insert_time = self._params.get(FIELD_ENDTIME, default_insettime)
        self.name = name
        if self.name is None:
            try:
                self.name = varname(self._frame, strict=False)
            except Exception as e:
                pass
        self._name = self.name
        self.type = TYPE_FIXEDTIME
        if FIELD_ENDTIME in list(inspect_.signature(self.fn).parameters.keys()):
            self.type = TYPE_REALTIME

    def __call__(self, **kwargs):
        """
        当实例被调用时，创建并返回一个新的Factor对象。
        该方法通过更新当前实例的状态，并使用延迟调用封装原始函数，创建一个新的Factor实例。
        如果新实例的类型为TYPE_D，则设置其结束时间为 15:00:00。

        Parameters
        ----------
        **kwargs : dict
            关键字参数，将传递给因子计算逻辑函数的参数

        Returns
        -------
        Factor
            一个新的Factor对象，其属性根据当前实例和调用参数初始化。
        """
        frame = self._frame + 1
        newFactor = Factor(*self._depends, fn=ygo.delay(self.fn)(**kwargs),
                           name=self._name,
                           frame=frame)
        newFactor.name = self.name
        newFactor.type = self.type
        if newFactor.type == TYPE_FIXEDTIME:
            newFactor.end_time = newFactor._params.get(FIELD_ENDTIME, self.end_time)
            # newFactor.insert_time = newFactor._params.get(FIELD_ENDTIME, self.insert_time)
            newFactor.insert_time = self.insert_time
        return newFactor

    def astype(self, _type: str):
        """有些因子因为没有实时数据的缘故，然而计算函数中使用了形参:`end_time`需要声明为日频因子"""
        self.type = _type
        return self

    def __repr__(self):
        # inspect(self, title=f"{self.name}", help=True)

        params = ygo.fn_params(self.fn)
        all_define_params = sorted(list(inspect_.signature(self.fn).parameters.keys()))

        default_params = {k: v for k, v in params}
        params_infos = list()
        for p in all_define_params:
            if p in default_params:
                params_infos.append(f'{p}={default_params[p]}')
            else:
                params_infos.append(p)
        params_infos = ', '.join(params_infos)
        mod = ygo.fn_path(self.fn)

        return f"""{mod}.{self.fn.__name__}({params_infos})"""

    @property
    def tb_name(self, ) -> str:
        tb_name = os.path.join("factors", self._name, f"version={self.version}")
        return tb_name

    def alias(self, name):
        """重新命名因子"""
        self.name = name
        return self

    def set_insert_time(self, insert_time):
        """
        设置因子的入库时间, 注意，设置了插入时间后，factor.type == "fixed_time"
        Parameters
        ----------
        insert_time: str
            入库时间，格式为 `hh:mm:ss`
        Returns
        -------
        Factor
            其他设置和原始因子一致，只是入库时间不同
        """
        frame = self._frame + 1
        newFactor = Factor(fn=self.fn, name=self._name, frame=frame).astype(TYPE_FIXEDTIME)
        newFactor.insert_time = insert_time
        newFactor.end_time = self.end_time
        return newFactor

    def set_end_time(self, end_time: str):
        """
        设置因子的结束时间
        Parameters
        ----------
        end_time: str
            结束时间，格式为 `hh:mm:ss`
        Returns
        -------
        Factor
            其他设置和原始因子一致，只是结束时间不同
        """
        frame = self._frame + 1
        newFactor = Factor(fn=self.fn, name=self._name, frame=frame)
        newFactor.end_time = end_time
        newFactor.insert_time = self.insert_time
        return newFactor

    def get_value(self,
                  date: str,
                  codes: list[str] | None = None,
                  time: str = '15:00:00',
                  avoid_future: bool = True,
                  rt: bool = True) -> pd.Series | pd.DataFrame | None:
        """
        获取指定日期和时间的最新数据。

        Parameters
        ----------
        date : str
            日期字符串，格式为 yyyy-mm-dd。
        codes : Iterable[str]
            证券代码列表，可选，默认为 None。
        time : str
            时间字符串，默认为 '15:00:00'。
        avoid_future: bool
            是否避免未来数据，默认 True
            - True: 当取值time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
            - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
        rt: bool
            是否实时取值，默认 True
            - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
            - False: fac保持原始设置, end_time保持不变

        Returns
        -------
        pandas.DataFrame | pandas.Series | None
            包含指定日期和时间的最新数据的 DataFrame。
            如果 只有一列数据，则返回 pandas.Series
        """
        try:
            return get_value(fac=self,
                             date=date,
                             codes=codes,
                             time=time,
                             avoid_future=avoid_future,
                             rt=rt, )
        except Exception as e:
            raise FactorGetError(fac_name=self.name,
                                 end_time=self.end_time,
                                 insert_time=self.insert_time,
                                 fac_params=self._params,
                                 get_date=date,
                                 get_time=time,
                                 error=e)

    def get_value_depends(self,
                          date: str,
                          codes: list[str] | None = None,
                          time: str = '15:00:00',
                          avoid_future: bool = True,
                          rt: bool = True):
        """
        获取依赖因子的值，并合并成一张宽表。

        Parameters
        ----------
        date : str
            日期字符串，用于获取因子值的日期, 格式为'yyyy-mm-dd'。
        codes : Iterable[str]
            可选的证券代码列表，默认为 None。
        time : str
            时间字符串，默认为 '15:00:00'。
        avoid_future: bool
            是否避免未来数据，默认 True
            - True: 当取值time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
            - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
        rt: bool
            是否实时取值，默认 True
            - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
            - False: fac保持原始设置, end_time保持不变

        Returns
        -------
        pandas DataFrame | None
            包含所有依赖因子值的宽表。

        Notes
        -----
        - 如果该因子不依赖于其他因子，则直接返回 None。
        - 函数会为每个因子获取其值，并将这些值合并成一个宽表。
        - 如果某个因子的值为 None，则跳过该因子。
        - 存在多列的因子，列名会被重命名，便于阅读与避免冲突, 命名规则为 {fac.name}.<columns>。
        - 最终的结果会根据 `with_date` 和 `with_time` 参数插入日期和时间列，并按日期和证券代码排序。
        """
        return get_value_depends(depends=self._depends,
                                 date=date,
                                 codes=codes,
                                 time=time,
                                 avoid_future=avoid_future,
                                 rt=rt, )

    def get_history(self,
                    date,
                    codes: list[str] | None = None,
                    period: str = '5d',
                    time='15:00:00',
                    avoid_future: bool = True,
                    rt: bool = True,
                    show_progress: bool = True,
                    n_jobs: int = 7):
        """
        回看period(包含当天), period最小单位为d, 小于d的周期向上取整，比如1d1s,视为2d

        Parameters
        ----------
        date : str
            结束日期, 格式 'yyyy-mm-dd'。
        codes : Iterable[str] | None
            可选的证券代码列表，默认为None。
        time : str
            时间，默认为'15:00:00', 格式 hh:mm:ss
        period: str
            回看周期, 最小单位为d, 小于d的周期向上取整，比如1d1s,视为2d
        n_jobs : int, optional
            并发任务数，默认为7。
        avoid_future: bool
            是否避免未来数据，默认 True
            - True: 当取值time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
            - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
        rt: bool
            是否实时取值，默认 True
            - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
            - False: fac保持原始设置, end_time保持不变
        show_progress: bool
            是否显示进度，默认True

        Returns
        -------
        polars.DataFrame | None

        Notes
        -----
        - 如果`avoid_future`=True 并且 指定的时间早于因子的结束时间，则将开始和结束日期都向前移动一个交易日。
        - 如果数据不完整，会自动补齐缺失的数据。
        - 最终结果会按日期和证券代码排序。
        """
        date_shifted, _ = xcals.shift_tradedt(date, self.end_time, period)
        beg_date, end_date = min([date, date_shifted]), max([date, date_shifted])
        res = get_history(fac=self,
                          beg_date=beg_date,
                          end_date=end_date,
                          codes=codes,
                          time=time,
                          avoid_future=avoid_future,
                          rt=rt,
                          show_progress=show_progress,
                          n_jobs=n_jobs)
        return res.filter(pl.col(FIELD_DATE) >= beg_date, pl.col(FIELD_DATE) <= end_date)

    def get_history_depends(self,
                            date,
                            codes: list[str] | None = None,
                            period: str = '5d',
                            times=('15:00:00',),
                            avoid_future: bool = True,
                            rt: bool = True,
                            show_progress: bool = True,
                            n_jobs=7):
        """
        回看依赖period(包含当天), period最小单位为d, 小于d的周期向上取整，比如1d1s,视为2d

        Parameters
        ----------
        date : str
            结束日期，格式为 'yyyy-mm-dd'。
        codes : Iterable[str]
            股票代码列表，默认为 None。
        period: str
            回看周期, 最小单位为d, 小于d的周期向上取整，比如1d1s,视为2d
        times : list[str]
            取值时间序列，默认为 ['15:00:00'], 格式为 'hh:mm:ss'
        show_progress : bool, optional
            是否显示进度条，默认为 True。
        n_jobs : int, optional
            并行任务数，默认为 7。
        avoid_future: bool
            是否避免未来数据，默认 True
            - True: 当取值time < fac.insert_time 时，取不到当天的数据，只能取上一个交易日的数据
            - False: 当取值 time < fac.insert_time 时, 可以取到当天的数据
        rt: bool
            是否实时取值，默认 True
            - True: `fac.end_time`是浮动的, 隐性设置 fac 为 fac(end_time=time)
            - False: fac保持原始设置, end_time保持不变
        show_progress : bool
            是否显示进度条，默认为 True。

        Returns
        -------
        polars.DataFrame | None

        Notes
        -----
        - 最终结果会按日期和股票代码排序。
        """
        date_shifted, _ = xcals.shift_tradedt(date, self.end_time, period)
        beg_date, end_date = min([date, date_shifted]), max([date, date_shifted])
        return get_history_depends(depends=self._depends,
                                   beg_date=beg_date,
                                   end_date=end_date,
                                   codes=codes,
                                   times=times,
                                   avoid_future=avoid_future,
                                   rt=rt,
                                   n_jobs=n_jobs,
                                   show_progress=show_progress, )

    def info(self, ):
        """
        打印因子计算函数的帮助文档以及因子的公开信息
        """
        inspect(self, help=True)
