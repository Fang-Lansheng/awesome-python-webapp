#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  :  Thistledown
@Contact :  120768091@qq.com
@Software:  PyCharm
@File    :  orm.py
@Time    :  2018/8/10 17:32
"""

import asyncio, logging
import aiomysql

'记录SQL操作'
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

'创建全局连接池， **kw：关键字参数集'
@asyncio.coroutine
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool   # 将__pool定义为全局变量
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),   # 主机IP，默认为localhost
        port=kw.get('port', 3306),          # 端口，默认3306
        user=kw['user'],                    # 用户
        password=kw['password'],            # 用户口令/密码
        db=kw['db'],                        # 选择数据库
        charset=kw.get('charset', 'utf-8'), # 设置数据库编码为UTF-8
        autocommit=kw.get('autocommit', True),  # 设置自动提交事务，默认打开
        maxsize=kw.get('maxsize', 10),      # 设置最大连接数，默认为10
        minsize=kw.get('minsize', 1),       # 设置最小连接数，默认为1
        loop=loop   # 需要传递一个事件循环实例，若无特别声明，默认使用asyncio.get_event_loop()
    )

'用select函数执行SELECT语句'
@asyncio.coroutine
async def select(sql, args, size=None):
    log(sql, args)  # 记录SQL操作
    global __pool   # 使用全局变量__pool
    async with __pool.get() as conn:    # 从连接池中获取一个链接🔗，使用完后自动释放
        async with conn.cursor(aiomysql.DictCursor) as cur: # 创建一个游标，返回由dict组成的list
            await cur.execute(sql.replace('?', '%s'), args or ())
            # SQL语句的占位符是“？”，而MySQL的占位符是“%s”，select函数在内部自动替换。
            # 注意始终坚持使用带参数的SQL，而不是自己拼接SQL字符串，这样可以防止SQL注入攻击
            if size:
                rs = await cur.fetchmany(size)  # 只读取size条记录
            else:
                rs = await cur.fetchall()       # 返回的rs是一个list，每一个元素是一个dict，代表一行记录
        logging.info('rows returned: %s' % len(rs))
        return rs

# 实现insert/update/delete语句，默认打开自动提交事务
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.acquire() as conn:    # 获取一个链接
        if not autocommit:
            await conn.begin()  # 协程开始启动
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur: # 创建一个游标，返回dict类型
                await cur.execute(sql.replace('?', '%s'), args) # 执行SQL
                affected = cur.rowcount # 获得影响的行数
            if not autocommit:
                await conn.commit() # 提交事务
        except BaseException as e:
            if not autocommit:
                await conn.rollback()   # 回滚当前启动的协程
            raise
        return affected

# 制作参数字符串
def create_args_string(num):
    L = []
    for n in range(num):    # SQL的占位符是“？”，num是多少就插入多少个占位符
        L.append('?')
    return ','.join(L)      # 将L拼接成字符串返回，例如num=3时："?,?,?"

# 定义数据类型的基类
class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        # 可传入参数对应列名、数据类型、主键、默认值
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):  # print(Field_object) 返回类名Field，数据类型，列名
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

# 继承Field类，定义字符类，默认变长100字节
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        # 可传入参数列名、主键、默认值、数据类型
        super().__init__(name, ddl, primary_key, default)   # 对应列名、数据类型、主键、默认值

# 继承Field类，定义Boolean类
class BooleanField(Field):
    def __init__(self, name=None, default=False):   # 可传入参数列名、默认值
        super().__init__(name, 'boolean', False, default)   # 对应列名、数据类型、主键、默认值

# 继承Field类，定义整数类（bigint），默认值为0
class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):  # 可传入参数列名、主键、默认值
        super().__init__(name, 'bigint', primary_key, default)    # 对应列名、参数类型、主键、默认值

# 继承Field类，定义浮点类（real），默认值为0.0
class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):  # 可传入参数列名、主键、默认值
        super().__init__(name, 'real', primary_key, default)    # 对应列名、数据类型、主键、默认值

# 继承Field类，定义text类
class TextField(Field):
    def __init__(self, name=None, default=None):    # 可传入参数列名、默认值
        super().__init__(name, 'text', False, default)  # 对应列名、数据类型、主键、默认值

# 定义元类
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):   # 用metaclass=ModelMetaclass创建类时，通过这个方法生成类
        if name=='Model':   # 定制Model类
            return type.__new__(cls, name, bases, attrs)    # 当前准备创建的类的对象、类的名字Model、类继承的父类集合、类的方法集合
        tableName = attrs.get('__table__', None) or name    # 获取表名，默认为None，或为类名
        logging.info('found model: %s (table: %s)' % (name, tableName)) # 类名、表名
        mappings = dict()   # 用于存储列名和对应的数据类型


