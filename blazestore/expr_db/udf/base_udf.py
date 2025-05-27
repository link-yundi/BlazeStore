# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/3/4 20:28
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

import polars as pl
import math

"""
基本算子：一元算子、二元算子、三元算子 以及 polars 支持的表达式(剔除数据泄露的)
"""
# ======================== 一元算子 ========================

def not_(expr: pl.Expr): return ~expr


def neg(expr: pl.Expr): return -expr


def abs(expr: pl.Expr): return expr.abs()


def log(expr: pl.Expr, base=math.e): return expr.log(base=base)


def sqrt(expr: pl.Expr): return expr.sqrt()


def square(expr: pl.Expr): return expr ** 2


def cube(expr: pl.Expr): return expr ** 3


def cbrt(expr: pl.Expr): return expr ** (1 / 3)


def sin(expr: pl.Expr): return expr.sin()

def sinh(expr: pl.Expr): return expr.sinh()

def arcsin(expr: pl.Expr): return expr.arcsin()

def arcsinh(expr: pl.Expr): return expr.arcsinh()


def cos(expr: pl.Expr): return expr.cos()

def cosh(expr: pl.Expr): return expr.cosh()

def arccos(expr: pl.Expr): return expr.arccos()

def arccosh(expr: pl.Expr): return expr.arccosh()

def tan(expr: pl.Expr): return expr.tan()

def tanh(expr: pl.Expr): return expr.tanh()

def arctan(expr: pl.Expr): return expr.arctan()

def arctanh(expr: pl.Expr): return expr.arctanh()


def sign(expr: pl.Expr): return expr.sign()


def sigmoid(expr: pl.Expr): return 1 / (1 + (-expr).exp())


# def all(expr: pl.Expr, ignore_nulls: bool = True): return expr.all(ignore_nulls=ignore_nulls)


# def any(expr: pl.Expr, ignore_nulls: bool = True): return expr.any(ignore_nulls=ignore_nulls)

def cot(expr: pl.Expr): return expr.cot()

def degrees(expr: pl.Expr): return expr.degrees()

def exp(expr: pl.Expr): return expr.exp()

def log1p(expr: pl.Expr): return expr.log1p()

def clip(expr: pl.Expr, lower_bound, upper_bound): return expr.clip(lower_bound, upper_bound)

# ======================== 二元算子 ========================
def add(left: pl.Expr, right: pl.Expr): return left + right


def sub(left: pl.Expr, right: pl.Expr): return left - right


def mul(left: pl.Expr, right: pl.Expr): return left * right


def div(left: pl.Expr, right: pl.Expr): return left / right


def floordiv(left: pl.Expr, right: pl.Expr): return left // right


def mod(left: pl.Expr, right: pl.Expr): return left % right


def lt(left: pl.Expr, right: pl.Expr): return left < right


def le(left: pl.Expr, right: pl.Expr): return left <= right


def gt(left: pl.Expr, right: pl.Expr): return left > right


def ge(left: pl.Expr, right: pl.Expr): return left >= right


def eq(left: pl.Expr, right: pl.Expr): return left == right


def neq(left: pl.Expr, right: pl.Expr): return left != right

def and_(left: pl.Expr, right: pl.Expr): return left & right

def or_(left: pl.Expr, right: pl.Expr): return left | right

def max(*exprs: pl.Expr): return pl.max_horizontal(*exprs)

def min(*exprs: pl.Expr): return pl.min_horizontal(*exprs)

def sum(*exprs: pl.Expr): return pl.sum_horizontal(*exprs)


# ======================== 三元 ========================
def if_(cond: pl.Expr, body: pl.Expr, or_else: pl.Expr):
    return pl.when(cond).then(body).otherwise(or_else)

def fib(high: pl.Expr, low: pl.Expr, ratio: float = 0.618):
    """
    计算裴波那契回调比率
    ratio: 0.236 | 0.382 | 0.618 等黄金分割比例
    """
    return low + (high - low) * ratio