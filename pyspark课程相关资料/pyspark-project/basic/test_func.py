#!/usr/bin/python
# -*- encoding:utf-8 -*-


def add(a, b=4):
    """
    函数的定义
        - 从冒号开始为函数体
        - 通过 缩进对齐 方式来表示同意代码块
    :param a: 第一个参数
    :param b: 第二参数
    :return: 相加返回值
    """
    # 求和
    sum_total = a + b
    # 返回
    return sum_total


if __name__ == '__main__':
    # 调用函数
    print add(10)

    # 两个参数
    print add(10, b=90)
