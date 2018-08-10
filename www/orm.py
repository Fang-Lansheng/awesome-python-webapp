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



