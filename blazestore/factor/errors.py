# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/5/26 14:35
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

from dataclasses import dataclass

@dataclass
class FactorGetError(Exception):

    fac_name: str
    end_time: str
    insert_time: str
    get_date: str
    get_time: str
    fac_params: dict
    error: Exception

    def __str__(self):
        return f"""
[因子名称]: {self.fac_name}
[因子时间]: {self.end_time}
[入库时间]: {self.insert_time}
[取值时间]: {self.get_date} {self.get_time}
[错误信息]: \n{self.error}
"""

    def __repr__(self):
        return self.__str__()
