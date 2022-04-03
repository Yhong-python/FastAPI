# coding=utf-8
"""
File: dev.py
Created on 2022/3/7 17:12
__author__= yangh
__remark__=
"""

from . import Config


class DevelopementConfig(Config):
    """开发模式下的配置"""
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:123456@192.168.110.139:3310/dbtools?charset=utf8"
    AUTO_CREATE_TABLE = 'ON'
    MYSQL_DB = 'students'
