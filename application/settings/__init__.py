# coding=utf-8
"""
File: __init__.py.py
Created on 2022/3/7 17:12
__author__= yangh
__remark__=
"""

class Config():
    """项目配置核心类"""
    # 调试模式
    DEBUG = False
    # 配置日志
    LOG_LEVEL = "DEBUG"

    # mysql数据库的配置信息
    # SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:123456@127.0.0.1:3306/students?charset=utf8"
    # 动态追踪修改设置，如未设置只会提示警告
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    # 查询时会显示原始SQL语句
    SQLALCHEMY_ECHO= False
    # 设置密钥，可以通过 base64.b64encode(os.urandom(48)) 来生成一个指定长度的随机字符串
    SECRET_KEY = "ghhBljAa0uzw2afLqJOXrukORE4BlkTY/1vaMuDh6opQ3uwGYtsDUyxcH62Aw3ju"
    # flask_session的配置信息
    PERMANENT_SESSION_LIFETIME = 24 * 60 * 60 # session 的有效期，单位是秒
    CSRF_ENABLED = True
    JSON_AS_ASCII= False
