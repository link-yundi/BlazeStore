# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/3/5 21:40
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path

import polars as pl
from toolz import partial

import ygo
import ylog
from .errors import CalculateError, CompileError, PolarsError, FailError
from .expr import Expr

# 动态加载模块
module_name = "udf"
module_path = Path(__file__).parent / "udf" / "__init__.py"
spec = importlib.util.spec_from_file_location(module_name, module_path)
module = importlib.util.module_from_spec(spec)
sys.modules[module_name] = module
spec.loader.exec_module(module)


@lru_cache(maxsize=512)
def parse_expr(expr: str) -> Expr:
    return Expr(expr)


class QDF:

    def __init__(self,
                 data: pl.LazyFrame | pl.DataFrame,
                 index: tuple[str] = ("date", "time", "asset"),
                 align: bool = True, ):
        assert isinstance(data, (pl.LazyFrame, pl.DataFrame)), "data must be a polars DataFrame or LazyFrame"
        self.data = data.with_columns(pl.col(pl.Decimal).cast(pl.Float32).round(5))
        if isinstance(self.data, pl.LazyFrame):
            self.data = self.data.collect()
        self.index = index
        self.dims = [self.data[name].drop_nulls().n_unique() for name in index]
        if align:
            lev_vals: list[pl.DataFrame] = [self.data.select(name).drop_nulls().unique() for name in index]
            full_index = lev_vals[0]
            for lev_val in lev_vals[1:]:
                full_index = full_index.join(lev_val, how="cross")
            self.data = full_index.join(self.data, on=index, how='left').sort(index)
        self.failed = list()
        self._expr_cache = dict()  # type: dict[Expr, str]
        self._cur_expr_cache = dict()
        self._data_: pl.LazyFrame = None

    def __str__(self):
        return self.data.__str__()

    def __repr__(self):
        return self.data.__str__()

    def register_udf(self, func: callable, name: str = None):
        name = name if name is not None else func.__name__
        setattr(module, name, func)

    def _compile_expr(self, expr: str, cover: bool):
        """str表达式 -> polars 表达式"""
        try:
            expr_parsed = Expr(expr)
            alias = expr_parsed.alias  # if expr_parsed.alias is not None else str(expr_parsed)
            current_cols = set(self.data.columns)
            # columns = self.data.columns
            if alias in current_cols and not cover:
                return pl.col(alias), alias
            # 如果该表达式已有对应列，直接复用
            if expr_parsed in self._expr_cache and not cover:
                expr_pl: pl.Expr = pl.col(self._expr_cache[expr_parsed]).alias(alias)
                return expr_pl, alias
            elif expr_parsed in self._cur_expr_cache and not cover:
                expr_pl: pl.Expr = pl.col(self._cur_expr_cache[expr_parsed]).alias(alias)
                return expr_pl, alias

            def recur_compile(expr_: Expr):
                """递归编译"""
                alias_ = expr_.alias
                if alias_ in current_cols and not cover:
                    # 已存在：直接select数据源
                    return pl.col(alias_)
                if expr_ in self._expr_cache:
                    return pl.col(self._expr_cache[expr_]).alias(alias_)
                elif expr_ in self._cur_expr_cache:
                    return pl.col(self._cur_expr_cache[expr_]).alias(alias_)
                func = getattr(module, expr_.fn_name)
                _params = ygo.fn_signature_params(func)
                if "dims" in _params:
                    func = partial(func, dims=self.dims)
                args = list()
                kwargs = dict()
                for arg in expr_.args:
                    if isinstance(arg, Expr):
                        args.append(recur_compile(arg))
                    elif isinstance(arg, dict):
                        kwargs.update(arg)
                    elif isinstance(arg, str):
                        args.append(pl.col(arg))
                    else:
                        args.append(arg)  # or args.append(pl.lit(arg))
                try:
                    expr_pl: pl.Expr = func(*args, **kwargs)
                    self._data_ = self._data_.with_columns(expr_pl.alias(alias_))
                    self._cur_expr_cache[expr_] = alias_
                    return pl.col(alias_)
                except Exception as e:
                    raise CompileError(message=f"{expr_.fn_name}({', '.join([str(arg) for arg in args])})\n{e}") from e

            return recur_compile(expr_parsed), alias
        except (CalculateError, CompileError, PolarsError) as e:
            raise e
        except Exception as e:
            # 所有未处理的错误统一抛出为 CompileError
            raise CompileError(message=f"[编译器外层]\n{e}") from e

    def sql(self, *exprs: str, cover: bool = False, ) -> pl.LazyFrame:
        """
        表达式查询
        Parameters
        ----------
        exprs: str
            表达式，比如 "ts_mean(close, 5) as close_ma5"
        cover: bool
            当遇到已经存在列名的时候，是否重新计算覆盖原来的列, 默认False，返回已经存在的列，跳过计算
            - True: 重新计算并且返回新的结果，覆盖掉原来的列
            - False, 返回已经存在的列，跳过计算
        Returns
        -------
            polars.DataFrame
        """
        self.failed = list()
        exprs_to_add = list()
        exprs_select = list()
        self._cur_expr_cache = {}
        self._data_ = self.data.lazy()

        for expr in exprs:
            try:
                compiled, alias = self._compile_expr(expr, cover)
                if compiled is not None:
                    exprs_to_add.append(compiled)
                    exprs_select.append(alias)
            except Exception as e:
                self.failed.append(FailError(expr, e))
        if self.failed:
            ylog.warning(f"QDF.sql 失败：{len(self.failed)}/{len(exprs)}: \n {self.failed}")
        self._data_ = self._data_.with_columns(*exprs_to_add).fill_nan(None)
        new_expr_cache = dict()
        try:
            self.data = self._data_.collect()
            final_df = self.data.select(*self.index, *exprs_select)
            current_cols = set(self.data.columns)
            # 缓存整理：只保留当前表达式的缓存
            self._expr_cache.update(self._cur_expr_cache)
            for k, v in self._expr_cache.items():
                if v in current_cols:
                    new_expr_cache[k] = v
            self._expr_cache = new_expr_cache
            return final_df
        except Exception as e:
            current_cols = set(self.data.columns)
            # 缓存整理：只保留当前表达式的缓存
            for k, v in self._expr_cache.items():
                if v in current_cols:
                    new_expr_cache[k] = v
            self._expr_cache = new_expr_cache
            raise PolarsError(message=f"LazyFrame.collect() 阶段出错\n{e}") from e

