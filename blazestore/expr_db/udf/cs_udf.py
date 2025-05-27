# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/3/4 20:20
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

import polars as pl
import numpy as np

over = dict(
    partition_by=["date", "time"],
    order_by=["asset", ]
)

EPS = 1e-12


def cs_ufit(expr: pl.Expr): return (expr - expr.median().over(**over)).abs()


def cs_rank(expr: pl.Expr): return expr.rank().over(**over)


def cs_demean(expr: pl.Expr): return expr - expr.mean().over(**over)


def cs_mean(expr: pl.Expr): return expr.mean().over(**over)


def cs_mid(expr: pl.Expr): return expr.median().over(**over)


def cs_moderate(expr: pl.Expr): return (expr - expr.mean().over(**over)).abs()


def cs_qcut(expr: pl.Expr, N=10):
    return expr.qcut(N, labels=[str(i) for i in range(1, N + 1)], allow_duplicates=True).cast(pl.Int32)


def cs_ic(left: pl.Expr, right: pl.Expr, ): return pl.corr(left, right, method="spearman").over(**over)


def cs_corr(left: pl.Expr, right: pl.Expr): return pl.corr(left, right, method="pearson").over(**over)


def cs_std(expr: pl.Expr): return expr.std().over(**over)


def cs_var(expr: pl.Expr): return expr.var().over(**over)


def cs_skew(expr: pl.Expr): return expr.skew().over(**over)


def cs_slope(left: pl.Expr, right: pl.Expr): return cs_corr(left, right) * cs_std(left) / cs_std(right)


def cs_resid(left: pl.Expr, right: pl.Expr): return left - cs_slope(left, right) * right

def cs_mad(expr: pl.Expr):
    return 1.4826 * (expr - expr.median()).abs().median().over(**over)

def cs_zscore(expr: pl.Expr): return (expr - cs_mean(expr)) / cs_std(expr)

def cs_norm(expr: pl.Expr): return (expr - cs_mid(expr))/cs_mad(expr)

def cs_midby(expr: pl.Expr, *by: pl.Expr): return expr.median().over(partition_by=[*over.get("partition_by"), *by], order_by=over.get("order_by"))

def cs_madby(expr: pl.Expr, *by: pl.Expr): return 1.4826 * (expr - expr.median()).abs().median().over(partition_by=[*over.get("partition_by"), *by], order_by=over.get("order_by"))

def cs_normby(expr: pl.Expr, *by: pl.Expr): return (expr - cs_midby(expr, *by))/(cs_madby(expr, *by)+EPS)

def cs_meanby(expr: pl.Expr, *by: pl.Expr): return expr.mean().over(partition_by=[*over.get("partition_by"), *by], order_by=over.get("order_by"))

def cs_stdby(expr: pl.Expr, *by: pl.Expr): return expr.std().over(partition_by=[*over.get("partition_by"), *by], order_by=over.get("order_by"))

def cs_sumby(expr: pl.Expr, *by: pl.Expr): return expr.sum().over(partition_by=[*over.get("partition_by"), *by], order_by=over.get("order_by"))


def cs_max(expr: pl.Expr): return expr.max().over(**over)


def cs_min(expr: pl.Expr): return expr.min().over(**over)


def cs_peakmax(expr: pl.Expr): return expr.peak_max().over(**over)

def cs_peakmin(expr: pl.Expr): return expr.peak_min().over(**over)

def cs_zscoreby(expr: pl.Expr, *by: pl.Expr): return (expr - cs_meanby(expr, *by)) / cs_stdby(expr, *by)

def cs_entropy(expr: pl.Expr): return expr.entropy().over(**over)

def cs_entropyby(expr: pl.Expr, *by: pl.Expr): return expr.entropy().over(partition_by=[*over.get("partition_by"), *by], order_by=over.get("order_by"))