# coding=utf-8
"""
File: apschedler_jobs.py
Created on 2022/3/25 11:41
__author__= yangh
__remark__=
"""

from application import db
from sqlalchemy.dialects.mysql import DOUBLE


class ApschedlerJobs(db.Model):
    """定时任务表"""
    __tablename__ = "apscheduler_jobs"
    id = db.Column(db.VARCHAR(collation='utf8_bin', length=191), primary_key=True, nullable=False, unique=True,
                   comment="定时任务id")
    # next_run_time = db.Column(db.Double(asdecimal=True), index=True,nullable=True,comment="下一次运行定时任务时间")
    next_run_time = db.Column(DOUBLE(asdecimal=True), index=True, nullable=True, comment="下一次运行定时任务时间")
    job_state = db.Column(db.BLOB(), nullable=False)


def get_job_obj_by_id(job_id):
    obj = ApschedlerJobs.query.filter(ApschedlerJobs.id==job_id).first()
    return obj
