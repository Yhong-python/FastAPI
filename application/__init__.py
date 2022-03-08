# coding=utf-8
"""
File: __init__.py.py
Created on 2022/3/7 17:11
__author__= yangh
__remark__=
"""

from flask import Flask
from application.settings.dev import DevelopementConfig
from application.settings.prod import ProductionConfig
from flask_cors import CORS
from flask_session import Session
from flask_sqlalchemy import SQLAlchemy
import logging
from logging.handlers import RotatingFileHandler

config = {
    "dev": DevelopementConfig,
    "prop": ProductionConfig,
}
db = SQLAlchemy()


# 把日志相关的配置封装成一个日志初始化函数
def setup_log(Config):
    # 设置日志的记录等级
    logging.basicConfig(level=Config.LOG_LEVEL)  # 调试debug级
    # 创建日志记录器，指明日志保存的路径、每个日志文件的最大大小、保存的日志文件个数上限
    file_log_handler = RotatingFileHandler("logs/log", maxBytes=1024 * 1024 * 300, backupCount=10)
    # 创建日志记录的格式 日志等级 输入日志信息的文件名 行数 日志信息
    formatter = logging.Formatter('%(levelname)s %(filename)s:%(lineno)d %(message)s')
    # 为刚创建的日志记录器设置日志记录格式
    file_log_handler.setFormatter(formatter)
    # 为全局的日志工具对象（flaskapp使用的）添加日志记录器
    logging.getLogger().addHandler(file_log_handler)


def init_app(config_name):
    """项目的初始化函数"""
    app = Flask('__name__')
    # 设置配置类
    Config = config[config_name]
    # 加载配置
    app.config.from_object(Config)
    # 配置数据库链接
    db.init_app(app)
    # 开启CSRF防范功能
    CORS(app)
    # 开启session功能
    Session(app)
    # 注册蓝图对象到app应用中
    from .apps.index import users_blu
    app.register_blueprint(users_blu, url_prefix='/users')
    setup_log(Config)

    return app
