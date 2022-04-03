# coding=utf-8
"""
File: 111.py
Created on 2022/3/7 17:03
__author__= yangh
__remark__=
"""
from application import init_app, db
from flask_script import Manager, Server
from flask_migrate import Migrate, MigrateCommand
import logging, sys, traceback
# 导入模型为了进行数据迁移，必须要导入
from application.models.configuration import Configuration
from application.models.tasks import Tasks
from application.models.projects import Projects
from application.models.task_history import TaskHistory
from application.models.apschedler_jobs import ApschedlerJobs

app = init_app('dev')
manager = Manager(app)
Migrate(app, db)
manager.add_command('db', MigrateCommand)
manager.add_command('start', Server(host='0.0.0.0', port=8000, use_debugger=True, use_reloader=False))  # 创建启动命令


@app.errorhandler(500)
def error_handler(e):
    """
    全局异常捕获
    """
    logging.error(traceback.format_exc())
    return {"code": 500, 'message': '未知异常'}, 500


if __name__ == '__main__':
    manager.run()
