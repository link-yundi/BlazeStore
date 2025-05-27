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
    partition_by=["date", "asset"],
    order_by=["time"]
)


def ind_mean(expr: pl.Expr, windows): return expr.rolling_mean(windows, min_samples=1).over(**over)


def ind_std(expr: pl.Expr, windows): return expr.rolling_std(windows, min_samples=1).over(**over)


def ind_sum(expr: pl.Expr, windows): return expr.rolling_sum(windows, min_samples=1).over(**over)


def ind_var(expr: pl.Expr, windows): return expr.rolling_var(windows, min_samples=1).over(**over)


def ind_skew(expr: pl.Expr, windows): return expr.rolling_skew(windows, ).over(**over)


def ind_ref(expr: pl.Expr, windows, dims):  # return expr.shift(int(abs(windows))).over(**over)
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x
                .to_numpy()
                .reshape(dims)
                .transpose((1, 0, 2))
                .reshape((dims[1], -1))
            )
            .shift(windows)
            .to_numpy()
            .reshape((dims[1], dims[0], dims[2]))
            .transpose((1, 0, 2))
            .ravel()
        )
        .replace(np.nan, None)
    )


def ind_mid(expr: pl.Expr, windows): return expr.rolling_median(windows, min_samples=1).over(**over)


def ind_mad(expr: pl.Expr, windows):
    return 1.4826 * (expr - expr.rolling_median(windows, min_samples=1)).abs().rolling_median(windows, min_samples=1).over(**over)


def ind_rank(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x
                .to_numpy()
                .reshape(dims)
                .transpose((1, 0, 2))
                .reshape((dims[1], -1))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").rank().last())
            .drop("index")
            .to_numpy()
            .reshape((dims[1], dims[0], dims[2]))
            .transpose((1, 0, 2))
            .ravel()
        )
    )

def ind_prod(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x
                .to_numpy()
                .reshape(dims)
                .transpose((1, 0, 2))
                .reshape((dims[1], -1))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").cum_prod())
            .drop("index")
            .to_numpy()
            .reshape((dims[1], dims[0], dims[2]))
            .transpose((1, 0, 2))
            .ravel()
        )
    )

def ind_max(expr: pl.Expr, windows): return expr.rolling_max(windows, min_samples=1).over(**over)


def ind_min(expr: pl.Expr, windows): return expr.rolling_min(windows, min_samples=1).over(**over)


def ind_ewmmean(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_mean(com=com,
                      span=span,
                      half_life=half_life,
                      alpha=alpha,
                      adjust=False,
                      min_samples=1)
            .over(**over))


def ind_ewmstd(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_std(com=com,
                     span=span,
                     half_life=half_life,
                     alpha=alpha,
                     adjust=False,
                     min_samples=1)
            .over(**over))


def ind_ewmvar(expr: pl.Expr, com=None, span=None, half_life=None, alpha=None):
    return (expr
            .ewm_var(com=com,
                     span=span,
                     half_life=half_life,
                     alpha=alpha,
                     adjust=False,
                     min_samples=1)
            .over(**over))


def ind_cv(expr: pl.Expr, windows): return ind_std(expr, windows) / ind_mean(expr, windows)


def ind_snr(expr: pl.Expr, windows): return ind_mean(expr, windows) / ind_std(expr,
                                                                              windows)  # 信噪比: signal_to_noise ratio


def ind_diff(expr: pl.Expr, windows=1): return expr.diff(windows).over(**over)


def ind_pct(expr: pl.Expr, windows=1): return expr.pct_change(windows).over(**over)


def ind_corr(left: pl.Expr, right: pl.Expr, windows): return pl.rolling_corr(left, right, window_size=windows,
                                                                             min_samples=1).over(**over)


def ind_cov(left: pl.Expr, right: pl.Expr, windows): return pl.rolling_cov(left, right, window_size=windows,
                                                                           min_samples=1).over(**over).replace(np.nan,
                                                                                                               None)

def ind_slope(left: pl.Expr, right: pl.Expr, windows): return (
            ind_mean(left * right, windows) - ind_mean(right, windows) * ind_mean(left, windows)) / ind_var(right,
                                                                                                            windows)


def ind_resid(left: pl.Expr, right: pl.Expr, windows): return right - ind_slope(left, right, windows) * right


def ind_quantile(expr: pl.Expr, windows, quantile):
    return expr.rolling_quantile(window_size=windows, quantile=quantile, min_samples=1).over(**over)

def ind_entropy(expr: pl.Expr, windows, dims):
    return (
        expr
        .map_batches(
            lambda x: pl.DataFrame(
                x
                .to_numpy()
                .reshape(dims)
                .transpose((1, 0, 2))
                .reshape((dims[1], -1))
            )
            .with_row_index()
            .rolling("index", period=f"{windows}i")
            .agg(pl.all().exclude("index").entropy())
            .drop("index")
            .to_numpy()
            .reshape((dims[1], dims[0], dims[2]))
            .transpose((1, 0, 2))
            .ravel()
        )
    )

def ind_zscore(expr: pl.Expr, windows):
    return (expr - ind_mean(expr, windows))/ind_std(expr, windows)

def ind_norm(expr: pl.Expr, windows):
    return (expr - ind_mid(expr, windows))/ind_mad(expr, windows)