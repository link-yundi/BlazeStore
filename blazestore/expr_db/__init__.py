# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/3/5 21:40
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
from __future__ import annotations

import math
from functools import wraps

import polars as pl

from .qdf import QDF


def signature(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        args = [str(arg) for arg in args]
        for k, v in kwargs.items():
            args.append(f"{k}={v}")
        string = f"{function.__name__}({', '.join(args)})"
        return string

    return wrapper


def from_polars(df: pl.DataFrame, index: tuple[str] = ("date", "time", "asset"), align: bool = True, ) -> QDF:
    """polars dataframe 转为 表达式数据库"""
    return QDF(df, index, align,)


@signature
def abs(expr: str): ...


@signature
def log(expr: str, base=math.e): ...


@signature
def sqrt(expr: str): ...


@signature
def square(expr: str): ...


@signature
def cube(expr: str): ...


@signature
def cbrt(expr: str): ...


@signature
def sin(expr: str): ...


@signature
def sinh(expr: str): ...


@signature
def arcsin(expr: str): ...


@signature
def arcsinh(expr: str): ...


@signature
def cos(expr: str): ...


@signature
def cosh(expr: str): ...


@signature
def arccos(expr: str): ...


@signature
def arccosh(expr: str): ...


@signature
def tan(expr: str): ...


@signature
def tanh(expr: str): ...


@signature
def arctan(expr: str): ...


@signature
def arctanh(expr: str): ...


@signature
def sign(expr: str): ...


@signature
def sigmoid(expr: str): ...


@signature
def cot(expr: str): ...


@signature
def degrees(expr: str): ...


@signature
def ind_entropy(expr: str, windows: int): ...

@signature
def d_entropy(expr: str, windows: int): ...

@signature
def cs_entropy(expr: str, windows: int): ...

@signature
def ts_entropy(expr: str, windows: int): ...


@signature
def exp(expr: str): ...


@signature
def log1p(expr: str): ...


@signature
def clip(expr: str, lower_bound, upper_bound): ...


@signature
def cs_ufit(expr: str): ...


@signature
def cs_rank(expr: str): ...


@signature
def cs_demean(expr: str): ...


@signature
def cs_mean(expr: str): ...


@signature
def cs_mid(expr: str): ...


@signature
def cs_moderate(expr: str): ...


@signature
def cs_qcut(expr: str, N: int = 10): ...


@signature
def cs_ic(left: str, right: str): ...


@signature
def cs_corr(left: str, right: str): ...


@signature
def cs_std(expr: str): ...


@signature
def cs_var(expr: str): ...


@signature
def cs_skew(expr: str): ...


@signature
def cs_slope(left: str, right: str): ...


@signature
def cs_resid(left: str, right: str): ...


@signature
def cs_zscore(expr: str): ...


@signature
def cs_midby(expr: str, *by: str): ...


@signature
def cs_meanby(expr: str, *by: str): ...


@signature
def cs_max(expr: str): ...


@signature
def cs_min(expr: str): ...


@signature
def cs_peakmax(expr: str): ...


@signature
def cs_peakmin(expr: str): ...


@signature
def ts_mean(expr: str, windows: int): ...


@signature
def ts_std(expr: str, windows: int): ...


@signature
def ts_sum(expr: str, windows: int): ...


@signature
def ts_var(expr: str, windows: int): ...


@signature
def ts_skew(expr: str, windows: int): ...


@signature
def ts_ref(expr: str, windows: int): ...


@signature
def ts_mid(expr: str, windows: int): ...


@signature
def ts_mad(expr: str, windows: int): ...


@signature
def ts_rank(expr: str, windows: int): ...


@signature
def ts_max(expr: str, windows: int): ...


@signature
def ts_min(expr: str, windows: int): ...


@signature
def ts_ewmmean(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def ts_ewmstd(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def ts_ewmvar(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def ts_cv(expr: str, windows: int): ...


@signature
def ts_snr(expr: str, windows: int): ...


@signature
def ts_diff(expr: str, windows: int): ...


@signature
def ts_pct(expr: str, windows: int): ...


@signature
def ts_slope(left: str, right: str, windows: int): ...


@signature
def ts_corr(left: str, right: str, windows: int): ...


@signature
def ts_cov(left: str, right: str, windows: int): ...


@signature
def ts_resid(left: str, right: str, windows: int): ...


@signature
def ts_quantile(expr: str, windows: int, q: float): ...


@signature
def ts_prod(expr: str, windows: int): ...


@signature
def d_mean(expr: str, windows: int): ...


@signature
def d_std(expr: str, windows: int): ...


@signature
def d_sum(expr: str, windows: int): ...


@signature
def d_var(expr: str, windows: int): ...


@signature
def d_skew(expr: str, windows: int): ...


@signature
def d_ref(expr: str, windows: int): ...


@signature
def d_mid(expr: str, windows: int): ...


@signature
def d_mad(expr: str, windows: int): ...


@signature
def d_rank(expr: str, windows: int): ...


@signature
def d_max(expr: str, windows: int): ...


@signature
def d_min(expr: str, windows: int): ...


@signature
def d_ewmmean(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def d_ewmstd(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def d_ewmvar(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def d_cv(expr: str, windows: int): ...


@signature
def d_snr(expr: str, windows: int): ...


@signature
def d_diff(expr: str, windows: int): ...


@signature
def d_pct(expr: str, windows: int): ...


@signature
def d_slope(left: str, right: str, windows: int): ...


@signature
def d_corr(left: str, right: str, windows: int): ...


@signature
def d_cov(left: str, right: str, windows: int): ...


@signature
def d_resid(left: str, right: str, windows: int): ...


@signature
def d_quantile(expr: str, windows: int, q: float): ...


@signature
def d_prod(expr: str, windows: int): ...


@signature
def ind_mean(expr: str, windows: int): ...


@signature
def ind_std(expr: str, windows: int): ...


@signature
def ind_sum(expr: str, windows: int): ...


@signature
def ind_var(expr: str, windows: int): ...


@signature
def ind_skew(expr: str, windows: int): ...


@signature
def ind_ref(expr: str, windows: int): ...


@signature
def ind_mid(expr: str, windows: int): ...


@signature
def ind_mad(expr: str, windows: int): ...


@signature
def ind_rank(expr: str, windows: int): ...


@signature
def ind_max(expr: str, windows: int): ...


@signature
def ind_min(expr: str, windows: int): ...


@signature
def ind_ewmmean(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def ind_ewmstd(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def ind_ewmvar(expr: str, com=None, span=None, half_life=None, alpha=None): ...


@signature
def ind_cv(expr: str, windows: int): ...


@signature
def ind_snr(expr: str, windows: int): ...


@signature
def ind_diff(expr: str, windows: int): ...


@signature
def ind_pct(expr: str, windows: int): ...


@signature
def ind_slope(left: str, right: str, windows: int): ...


@signature
def ind_corr(left: str, right: str, windows: int): ...


@signature
def ind_cov(left: str, right: str, windows: int): ...


@signature
def ind_resid(left: str, right: str, windows: int): ...


@signature
def ind_quantile(expr: str, windows: int, q: float): ...


@signature
def ind_prod(expr: str, windows: int): ...


@signature
def max(*exprs: str): ...


@signature
def min(*exprs: str): ...


@signature
def sum(*exprs: str): ...
