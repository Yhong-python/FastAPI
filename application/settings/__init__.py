# coding=utf-8
"""
File: __init__.py.py
Created on 2022/3/7 17:12
__author__= yangh
__remark__=
"""
import pytz
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor


class Config():
    """项目配置核心类"""
    DEBUG = False
    LOG_LEVEL = "DEBUG"
    # SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:123456@127.0.0.1:3306/dbtools?charset=utf8"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    SECRET_KEY = "ghhBljAa0uzw2afLqJOXrukORE4BlkTY/1vaMuDh6opQ3uwGYtsDUyxcH62Aw3ju"
    PERMANENT_SESSION_LIFETIME = 24 * 60 * 60
    CSRF_ENABLED = True
    JSON_AS_ASCII = False
    # 定时任务配置
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(url="mysql+pymysql://root:yanghong1994@localhost:3306/dbtools?charset=utf8")}
    # 线程池配置，最大5个线程
    SCHEDULER_EXECUTORS = {'default': ThreadPoolExecutor(5)}
    # 调度开关开启
    SCHEDULER_API_ENABLED = True
    # 设置容错时间为 10分钟
    SCHEDULER_JOB_DEFAULTS = {'misfire_grace_time': 600}
    # 配置时区
    # SCHEDULER_TIMEZONE = 'Asia/Shanghai'
    SCHEDULER_TIMEZONE=pytz.timezone('Asia/Shanghai')
