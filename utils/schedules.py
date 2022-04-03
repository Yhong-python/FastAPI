# coding=utf-8
"""
File: schedules.py
Created on 2022/3/24 14:03
__author__= yangh
__remark__=
"""
from flask import current_app
from flask_apscheduler import APScheduler as _BaseAPScheduler
# from apscheduler.schedulers.background import BackgroundScheduler


# 重写APScheduler，实现上下文管理机制，对于任务函数涉及数据库操作有用
class APScheduler(_BaseAPScheduler):
    def get_app(self):
        if self.app:
            return self.app
        else:
            return current_app

    def run_job(self, id, jobstore=None):
        with self.app.app_context():
            super().run_job(id=id, jobstore=jobstore)

scheduler = APScheduler()
# scheduler = APScheduler(scheduler=BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai')))
