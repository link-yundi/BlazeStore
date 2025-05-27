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
    partition_by=["time", "asset"],
    order_by=["date"]
)


def d_mean(expr: pl.Expr, windows): return expr.rolling_mean(windows, min_samples=1).over(**over)


def d_std(expr: pl.Expr, windows): return expr.rolling_std(windows, min_samples=1).over(**over)


def d_sum(expr: pl.Expr, windows): return expr.rolling_sum(windows, min_samples=1).over(**over)


def d_var(expr: pl.Expr, windows): return expr.rolling_var(windows, min_samples=1).over(**over)


def d_skew(expr: pl.Expr, windows): return expr.rolling_skew(windows, ).over(**over)


def d_ref(expr: pl.Expr, windows, dims):  # return expr.shift(int(abs(windows))).over(**over)
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((dims[0], -1))
            )
            .shift(windows)
            .to_numpy()
            .ravel()
        )
        .replace(np.nan, None)
    )


def d_mid(expr: pl.Expr, windows): return expr.rolling_median(windows, min_samples=1).over(**over)


def d_mad(expr: pl.Expr, windows):
    return (expr-expr.rolling_median(windows, min_samples=1)).abs().rolling_median(windows, min_samples=1).over(**over)


def d_rank(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((dims[0], -1))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").rank().last())
            .drop("index")
            .to_numpy()
            .ravel()
        )
    )


def d_prod(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((dims[0], -1))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").cum_prod())
            .drop("index")
            .to_numpy()
            .ravel()
        )
    )


def d_max(expr: pl.Expr, windows): return expr.rolling_max(windows, min_samples=1).over(**over)


def d_min(expr: pl.Expr, windows): return expr.rolling_min(windows, min_samples=1).over(**over)


def d_ewmmean(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_mean(com=com,
                      span=span,
                      half_life=half_life,
                      alpha=alpha,
                      adjust=False,
                      min_samples=1)
            .over(**over))


def d_ewmstd(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_std(com=com,
                     span=span,
                     half_life=half_life,
                     alpha=alpha,
                     adjust=False,
                     min_samples=1)
            .over(**over))


def d_ewmvar(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_var(com=com,
                     span=span,
                     half_life=half_life,
                     alpha=alpha,
                     adjust=False,
                     min_samples=1)
            .over(**over))


def d_cv(expr: pl.Expr, windows): return d_std(expr, windows) / d_mean(expr, windows)


def d_snr(expr: pl.Expr, windows): return d_mean(expr, windows) / d_std(expr, windows)  # 信噪比: signal_to_noise ratio


def d_diff(expr: pl.Expr, windows=1): return expr.diff(windows).over(**over)


def d_pct(expr: pl.Expr, windows=1): return expr.pct_change(windows).over(**over)


def d_corr(left: pl.Expr, right: pl.Expr, windows): return pl.rolling_corr(left, right, window_size=windows,
                                                                           min_samples=1).over(**over)


def d_cov(left: pl.Expr, right: pl.Expr, windows): return pl.rolling_cov(left, right, window_size=windows,
                                                                         min_samples=1).over(**over).replace(np.nan,
                                                                                                             None)


def d_slope(left: pl.Expr, right: pl.Expr, windows): return (
        d_mean(left * right, windows) - d_mean(right, windows) * d_mean(left, windows)) / d_var(right, windows)


def d_resid(left: pl.Expr, right: pl.Expr, windows): return right - d_slope(left, right, windows) * right


def d_quantile(expr: pl.Expr, windows, quantile):
    return expr.rolling_quantile(window_size=windows, quantile=quantile, min_samples=1).over(**over)

def d_entropy(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x.to_numpy().reshape((dims[0], -1))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").entropy())
            .drop("index")
            .to_numpy()
            .ravel()
        )
    )

def d_zscore(expr: pl.Expr, windows):
    return (expr - d_mean(expr, windows))/d_std(expr, windows)
