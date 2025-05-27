# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2024/11/6 下午7:25
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import sqlparse
import re

def format_sql(sql_content):
    """将sql语句进行规范化，并去除sql中的注释，输入和输出均为字符串"""
    parse_str = sqlparse.format(sql_content, reindent=True, strip_comments=True)
    return parse_str

def extract_temp_tables(with_clause):
    """从WITH子句中提取临时表名，输出为列表"""
    temp_tables = re.findall(r'\b(\w+)\s*as\s*\(', with_clause, re.IGNORECASE)
    return temp_tables

def extract_table_names_from_sql(sql_query):
    """从sql中提取对应的表名称，输出为列表"""
    table_names = set()
    # 解析SQL语句
    parsed = sqlparse.parse(sql_query)
    # 正则表达式模式，用于匹配表名
    table_name_pattern = r'\bFROM\s+([^\s\(\)\,]+)|\bJOIN\s+([^\s\(\)\,]+)'

    # 用于存储WITH子句中的临时表名
    remove_with_name = []

    # 遍历解析后的语句块
    for statement in parsed:
        # 转换为字符串
        statement_str = str(statement)# .lower()

        # 将字符串中的特殊语法置空
        statement_str = re.sub(r'(substring|extract)\s*\(((.|\s)*?)\)', '', statement_str)

        # 查找匹配的表名
        matches = re.findall(table_name_pattern, statement_str, re.IGNORECASE)

        for match in matches:
            # 提取非空的表名部分
            for name in match:
                if name:
                    # 对于可能包含命名空间的情况，只保留最后一部分作为表名
                    table_name = name.split('.')[-1]
                    # 去除表名中的特殊符号
                    table_name = re.sub(r'("|`|\'|;)', '', table_name)
                    table_names.add(table_name)

        # 处理特殊的WITH语句
        if 'with' in statement_str:
            remove_with_name = extract_temp_tables(statement_str)
    # 移除多余的表名
    if remove_with_name:
        table_names = list(set(table_names) - set(remove_with_name))

    return table_names



