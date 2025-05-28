# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/26 14:35
@author: ZhangYundi; HuangBogeng
@email: yundi.xxii@outlook.com; huangbogeng@outlook.com
---------------------------------------------
"""

from .core import Factor

from . import errors


class FIELD:
    DATE = "date"
    TIME = "time"
    ASSET = "asset"
    VERSION = "version"
    ENDTIME = "end_time"


class TYPE:
    FIXEDTIME = "fixed_time"  # 因子插入时间是固定的
    REALTIME = "real_time"  # 因子插入时间是实时的


class FORMAT:
    DATE = "%Y-%m-%d"
    TIME = "%H:%M:%S"


INDEX = (
    FIELD.DATE,
    FIELD.TIME,
    FIELD.ASSET,
)


__all__ = ["FIELD", "TYPE", "FORMAT", "INDEX", "errors", "Factor"]
