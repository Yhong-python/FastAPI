# coding=utf-8
from datetime import datetime
from application import db


class Result(db.Model):
    """已保存任务的执行历史记录"""
    __tablename__ = "task_result_history"
    id = db.Column(db.Integer, primary_key=True, comment="主键ID")
    task_id = db.Column(db.Integer, comment="对应任务id")
    create_time = db.Column(db.DateTime, default=datetime.now, comment="创建时间")
    update_time = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    compare_result = db.Column(db.Integer, comment="比对结果 0：差异 1：相同")
    compare_detail = db.Column(db.JSON, comment="比对详情")

    except_field_one = db.Column(db.String(255), comment="预留字段1")
    except_field_two = db.Column(db.String(255), comment="预留字段2")


def get_result_obj(task_id, compare_dict):
    """
    在保存任务的时候，将本次比对结果，作为对象返回，将结果表与任务表在同一事务下进行处理
    @param task_id: 对应任务id
    @param compare_dict: 比对结果
    @return:
    """
    result = Result(task_id=task_id, compare_result=compare_dict['result'], compare_detail=compare_dict)
    return result






