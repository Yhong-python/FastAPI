# coding=utf-8
"""
File: __init__.py.py
Created on 2022/3/7 17:11
__author__= yangh
__remark__=
"""
import socket

from flask import Flask, Response, jsonify
from application.settings.dev import DevelopementConfig
from application.settings.prod import ProductionConfig
from flask_cors import CORS
from flask_session import Session
from flask_sqlalchemy import SQLAlchemy
import logging
from logging.handlers import RotatingFileHandler
from utils.response import JSONEncoder
import os, time
from utils.schedules import scheduler

config = {
    "dev": DevelopementConfig,
    "prop": ProductionConfig,
}
db = SQLAlchemy()


class JSONResponse(Response):
    default_mimetype = "application/json"

    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, dict):
            response = jsonify(response)
        return super(JSONResponse, cls).force_type(response, environ)


# log配置，按日期生成日志文件
def make_dir(make_dir_path):
    path = make_dir_path.strip()
    if not os.path.exists(path):
        os.makedirs(path)


def setup_log(Config):
    logging.basicConfig(level=Config.LOG_LEVEL)
    log_dir_name = "logs"
    log_file_name = time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.log'
    log_file_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir)) + os.sep + log_dir_name
    make_dir(log_file_folder)
    log_file_str = log_file_folder + os.sep + log_file_name
    file_log_handler = RotatingFileHandler(log_file_str, maxBytes=1024 * 1024 * 300, backupCount=10, encoding='UTF-8')
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)


def init_app(config_name):
    """项目的初始化函数"""
    app = Flask('__name__')
    Config = config[config_name]
    app.config.from_object(Config)
    app.response_class = JSONResponse
    app.json_encoder = JSONEncoder
    #解决uwsgi多线程多进程部署后定时任务重复执行的问题
    # try:
    #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     sock.bind(("127.0.0.1", 47200))
    # except socket.error:
    #     print( "!!!scheduler already started, DO NOTHING")
    # else:
    #     # scheduler.init_app(app)
    #     scheduler.start()
    #     print("scheduler started")
    # 初始化定时器
    scheduler.init_app(app)
    # 启动定时器，默认后台启动
    scheduler.start()
    db.init_app(app)
    CORS(app)
    Session(app)
    from .apps.dbTools import dbtools_blu
    app.register_blueprint(dbtools_blu, url_prefix='/dbTools')
    setup_log(Config)
    return app
