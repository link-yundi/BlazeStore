# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/16 10:47
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

from dataclasses import dataclass

@dataclass
class ParseError(Exception):
    message: str

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()

@dataclass
class CalculateError(Exception):
    message: str

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()

@dataclass
class CompileError(Exception):
    message: str

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()

@dataclass
class PolarsError(Exception):
    message: str

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()

@dataclass
class FailError:
    expr: str
    error: Exception

    def __str__(self):
        return f"""
[失败表达式]: {self.expr}
[错误类型]: {self.error.__class__.__name__}
[错误信息]: \n{self.error}
"""

    def __repr__(self):
        return self.__str__()