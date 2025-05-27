# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2025/3/3 19:52
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass

from lark import Lark, Transformer, v_args

from .errors import ParseError


# 基类
class Token:
    pass


@dataclass
class OperatorToken(Token):
    """算子类型token"""
    value: str


@dataclass
class OperandToken(Token):
    """运算对象token"""
    value: str | float | int


with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    grammar = """
        start: expr
        ?expr: ternary_expr
        ?ternary_expr: or_expr
            | or_expr "?" or_expr ":" ternary_expr -> ternary
        ?or_expr: and_expr
            | or_expr "|" and_expr -> or_
        ?and_expr: comp_expr
            | and_expr "&" comp_expr -> and_
        ?comp_expr: eq_expr
            | comp_expr "<" eq_expr -> lt
            | comp_expr ">" eq_expr -> gt
            | comp_expr "<=" eq_expr -> le
            | comp_expr ">=" eq_expr -> ge
        ?eq_expr: arith_expr
            | eq_expr "==" arith_expr -> eq
            | eq_expr "!=" arith_expr -> neq
        ?arith_expr: term
            | arith_expr "+" term -> add
            | arith_expr "-" term -> sub
        ?term: pow_expr
            | term "*" pow_expr -> mul
            | term "/" pow_expr -> div
            | term "//" pow_expr -> floordiv // 取整
            | term "%" pow_expr -> mod      // 求余
        ?pow_expr: factor
            | factor "**" pow_expr -> pow
        ?factor: atom
            | "-" factor -> neg
            | "!" factor -> not_
            | "~" factor -> not_
        ?atom: function
            | NAME
            | NUMBER
            | FLOAT
            | "(" expr ")"
            | implicit_mul // 隐式乘法
            | attribute_access // 新增：属性访问
        implicit_mul: (NUMBER | FLOAT) NAME -> implicit_mul  // 隐式乘法
        attribute_access: atom "." NAME -> attribute_access // 新增：属性访问
        function: NAME "(" expr_list ")" -> function
        // expr_list: expr ("," expr)*
        keyword_arg: NAME "=" expr -> keyword_arg  // 关键字参数
        expr_list: (expr | keyword_arg) ("," (expr | keyword_arg))*  // 支持关键字参数
        NAME: /[a-zA-Z_$,][a-zA-Z0-9_$]*/
        NUMBER: /\d+/  // regex for numbers
        FLOAT: /\d+\.\d+([eE][+-]?\d+)?/ | /\d+[eE][+-]?\d+/  // 支持科学计数法
        %import common.WS
        %ignore WS
    """


class ExprParser(Transformer):
    @v_args(inline=True)
    def ternary(self, a, b, c):
        return Expr.new("if_", [a, b, c])

    def attribute_access(self, items):
        return ".".join(items)

    def keyword_arg(self, item):
        k, v = item
        return {k: v}

    def NAME(self, name):
        return str(name)

    def NUMBER(self, number):  # new transformer for numbers
        return int(number)

    def FLOAT(self, number):
        return float(number)

    def add(self, items):
        return Expr.new("add", items)

    def sub(self, items):
        return Expr.new("sub", items)

    def mul(self, items):
        return Expr.new("mul", items)

    def div(self, items):
        return Expr.new("div", items)

    def floordiv(self, items):
        return Expr.new("floordiv", items)

    def mod(self, items):
        return Expr.new("mod", items)

    def pow(self, items):
        return Expr.new("pow", items)

    def neg(self, items):
        item = items[0]
        if isinstance(item, (int, float)):
            return -item
        return Expr.new("neg", items)

    def not_(self, item):
        return Expr.new("not_", item)

    def and_(self, items):
        return Expr.new("and_", items)

    def or_(self, items):
        return Expr.new("or_", items)

    def eq(self, items):
        return Expr.new("eq", items)

    def neq(self, items):
        return Expr.new("neq", items)

    def lt(self, items):
        return Expr.new("lt", items)

    def gt(self, items):
        return Expr.new("gt", items)

    def le(self, items):
        return Expr.new("le", items)

    def ge(self, items):
        return Expr.new("ge", items)

    def function(self, items):
        name = items.pop(0)
        return Expr.new(name, items[0])

    def implicit_mul(self, items):
        return Expr.new("mul", items)

    def expr_list(self, items):
        return items


parser = Lark(grammar, parser='lalr', transformer=ExprParser())


def parse_expr(expression: str) -> Expr:
    return parser.parse(expression).children[0]

class Expr:

    def __init__(self, expr: str | None = None):

        self.fn_name: str | None = ""
        self.args: list | None = None
        self.alias: str | None = None
        if expr:
            try:
                self._parse(expr)
            except Exception as e:
                raise ParseError(f"{expr}\n{e}")

    @classmethod
    def new(cls, fn_name: str | None, args: list | None, alias: str | None = None):
        expr = cls()
        expr.fn_name = fn_name
        expr.args = args
        expr.alias = alias if alias is not None else str(expr)
        return expr

    def __hash__(self):
        return hash(str(self).strip())

    def __eq__(self, other):
        return str(self).strip() == str(other).strip()


    def to_rpn(self) -> list[Token]:
        """生成逆波兰表达式: (后缀表达式: 运算符在后)"""
        rpn = list()

        # 递归遍历子表达式
        def _traverse(node: Expr):

            if node.args is not None:
                for child in node.args:
                    if isinstance(child, Expr):
                        _traverse(child)
                    else:
                        rpn.append(OperandToken(child))
            rpn.append(OperatorToken(node.fn_name))

        _traverse(self)

        return rpn

    def __str__(self):
        unary_map = {"neg": "-", "not_": "!"}
        binary_map = {"add": "+",
                      "mul": "*",
                      "div": "/",
                      "sub": "-",
                      "floordiv": "//",
                      "mod": "%",
                      "pow": "**",
                      "and_": "&",
                      "or_": "|",
                      "gt": ">",
                      "gte": ">=",
                      "lt": "<",
                      "lte": "<=",
                      "eq": "==",
                      "neq": "!=",
                      }
        if self.fn_name is None:
            return str(self.args[0])
        if self.fn_name == "if_":
            cond, body, orelse = self.args
            return f"{cond}?{body}:{orelse}"
        elif self.fn_name in ("neg", "not_"):
            return f"{unary_map.get(self.fn_name)}{self.args[0]}"
        elif self.fn_name in binary_map:
            return f"({binary_map.get(self.fn_name).join([str(arg) for arg in self.args])})"
        else:
            return f"{self.fn_name}({', '.join([str(arg) for arg in self.args])})"

    def __repr__(self):
        return self.__str__()

    def _parse(self, expr):
        """
        解析表达式
        """
        convertor = {
            'if(': 'if_(',
            'not(': 'not_(',
            'and(': 'and_(',
            'or(': 'or_(',
            '$': '',
            "\n": '',
            "!": "~",
            ",": ", ",
        }
        for old, new in convertor.items():
            expr = expr.replace(old, new)
        new_expr = expr
        match = re.search(r'(?i)(.+?)\s+AS\s+(\w+)', new_expr)
        alias = None
        if match:
            new_expr = match.group(1).strip()
            alias = match.group(2).strip()

        expr_ = parse_expr(new_expr)
        self.alias = alias if alias is not None else str(expr_)
        if not isinstance(expr_, Expr):
            self.args = [expr_]
        else:
            self.fn_name, self.args = expr_.fn_name, expr_.args


    @property
    def n_args(self) -> int:
        """返回表达式的参数个数"""
        return len(self.args)

    @property
    def depth(self) -> int:
        """返回表达式的嵌套深度"""
        _depth = 1
        _depths = [0]
        for arg in self.args:
            if isinstance(arg, Expr):
                _depths.append(arg.depth)
        return _depth + max(_depths)
