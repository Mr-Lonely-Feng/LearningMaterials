#!/usr/bin/python
# -*- encoding:utf-8 -*-


if __name__ == '__main__':
    # 列表list
    number_list = [1, 2, 3, 4, 5]

    # 使用FOR 循环进行打印
    for num in number_list:
        print num
    print '================================='

    """
        在SCALA中集合List里面有很多函数，都是高阶函数，函数的参数类型为函数
    """
    # 对上述列表中的每个元素进行平方
    new_list = map(lambda x: x * x, number_list)

    for num in new_list:
        print num

    # 如何获取list中的元素
    print new_list[0]
