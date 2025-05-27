# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/3/5 01:04
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import numpy as np
import polars as pl

over = dict(
    partition_by=["asset"],
    order_by=["date", "time"]
)


def ts_mean(expr: pl.Expr, windows): return expr.rolling_mean(windows, min_samples=1).over(**over)


def ts_std(expr: pl.Expr, windows): return expr.rolling_std(windows, min_samples=1).over(**over)


def ts_sum(expr: pl.Expr, windows): return expr.rolling_sum(windows, min_samples=1).over(**over)


def ts_var(expr: pl.Expr, windows): return expr.rolling_var(windows, min_samples=1).over(**over)


def ts_skew(expr: pl.Expr, windows): return expr.rolling_skew(windows, ).over(**over)


def ts_ref(expr: pl.Expr, windows, dims):  # return expr.shift(int(abs(windows))).over(**over)
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((-1, dims[-1]))
            )
            .shift(windows)
            .to_numpy()
            .ravel()
        )
        .replace(np.nan, None)
    )


def ts_mid(expr: pl.Expr, windows): return expr.rolling_median(windows, min_samples=1).over(**over)


def ts_mad(expr: pl.Expr, windows):
    return 1.4826 * (expr - expr.rolling_median(windows, min_samples=1)).abs().rolling_median(windows, min_samples=1).over(**over)


def ts_rank(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((-1, dims[-1]))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").rank().last())
            .drop("index")
            .to_numpy()
            .ravel()
        )
    )

def ts_prod(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((-1, dims[-1]))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").cum_prod())
            .drop("index")
            .to_numpy()
            .ravel()
        )
    )


def ts_max(expr: pl.Expr, windows): return expr.rolling_max(windows, min_samples=1).over(**over)


def ts_min(expr: pl.Expr, windows): return expr.rolling_min(windows, min_samples=1).over(**over)


def ts_ewmmean(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_mean(com=com,
                      span=span,
                      half_life=half_life,
                      alpha=alpha,
                      adjust=False,
                      min_samples=1)
            .over(**over))


def ts_ewmstd(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_std(com=com,
                     span=span,
                     half_life=half_life,
                     alpha=alpha,
                     adjust=False,
                     min_samples=1)
            .over(**over))


def ts_ewmvar(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_var(com=com,
                     span=span,
                     half_life=half_life,
                     alpha=alpha,
                     adjust=False,
                     min_samples=1)
            .over(**over))


def ts_cv(expr: pl.Expr, windows): return ts_std(expr, windows) / ts_mean(expr, windows)


def ts_snr(expr: pl.Expr, windows): return ts_mean(expr, windows) / ts_std(expr, windows)  # 信噪比: signal_to_noise ratio


def ts_diff(expr: pl.Expr, windows=1): return expr.diff(windows).over(**over)


def ts_pct(expr: pl.Expr, windows=1): return expr.pct_change(windows).over(**over)


def ts_corr(left: pl.Expr, right: pl.Expr, windows): return pl.rolling_corr(left, right, window_size=windows,
                                                                            min_samples=1).over(**over)


def ts_cov(left: pl.Expr, right: pl.Expr, windows): return pl.rolling_cov(left, right, window_size=windows,
                                                                          min_samples=1).over(**over).replace(np.nan,
                                                                                                              None)


def ts_slope(left: pl.Expr, right: pl.Expr, windows): return (
            ts_mean(left * right, windows) - ts_mean(right, windows) * ts_mean(left, windows)) / ts_var(right, windows)


def ts_resid(left: pl.Expr, right: pl.Expr, windows): return right - ts_slope(left, right, windows) * right


def ts_quantile(expr: pl.Expr, windows, quantile):
    return expr.rolling_quantile(window_size=windows, quantile=quantile, min_samples=1).over(**over)

def ts_entropy(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((-1, dims[-1]))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").entropy())
            .drop("index")
            .to_numpy()
            .ravel()
        )
    )

def ts_zscore(expr: pl.Expr, windows):
    return (expr - ts_mean(expr, windows))/ts_std(expr, windows)
