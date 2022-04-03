# coding=utf-8
from datetime import datetime
from application import db
from application.models.projects import Projects
from utils.response_utils import ResponseUtils
import logging, traceback


class TaskHistory(db.Model):
    """已保存任务的执行历史记录"""
    __tablename__ = "task_history"
    id = db.Column(db.Integer, primary_key=True, comment="主键ID")
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id', ondelete='CASCADE'),
                        comment="所属项目id")  # task_id关联task_list表
    create_time = db.Column(db.DateTime, default=datetime.now, comment="创建时间")
    update_time = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    compare_result = db.Column(db.Integer, comment="比对结果 0：差异 1：相同 -1：定时任务异常")
    result_detail = db.Column(db.JSON, default=None,comment="对比的结果详情")
    # except_field_one = db.Column(db.String(255), comment="预留字段1")
    except_field_two = db.Column(db.String(255), comment="预留字段2")


def get_history_obj(task_id, compare_result):
    """
    在保存任务的时候，将本次比对结果，作为对象返回，将结果表与任务表在同一事务下进行处理
    @param task_id: 对应任务id
    @param compare_dict: 比对结果
    @return:
    """
    result = TaskHistory(task_id=task_id, compare_result=compare_result)
    return result


def get_task_list():
    sql = """
    SELECT t.id,p.`name`,t.task_name,t.task_time,temp.compare_result,temp.create_time
    FROM tasks t
    LEFT JOIN projects p ON t.project_id=p.id
    LEFT JOIN (SELECT th.task_id task_id,MAX(th.create_time) create_time,th.compare_result compare_result
    FROM task_history th
    GROUP BY th.task_id) temp on t.id=temp.task_id
    ORDER BY p.`name`
    """
    sql_result = db.session.execute(sql)
    result = []
    for data in list(sql_result):
        temp_dict = {}
        temp_dict['task_id'] = data[0]
        temp_dict['project_name'] = data[1]
        temp_dict['task_name'] = data[2]
        temp_dict['task_time'] = data[3]
        temp_dict['compare_result'] = data[4]
        temp_dict['create_time'] = data[5]
        result.append(temp_dict)
    result=sorted(result,key=lambda x:x['task_name'])
    return result


def get_task_run_history_last5(task_id):
    """
    获取任务最新的5条运行记录
    @param task_id:
    @return:
    """
    tasks_last_5 = db.session.query(TaskHistory.compare_result, TaskHistory.create_time).filter(
        TaskHistory.task_id == task_id).order_by(TaskHistory.create_time.desc()).limit(5).all()
    result = []
    for data in tasks_last_5:
        temp_dict = {}
        temp_dict['compare_result'] = data[0]
        temp_dict['create_time'] = data[1]
        result.append(temp_dict)
    result=sorted(result,key=lambda x:x['create_time'],reverse=True)
    return result


def insert_task(task_id, flag,result_detail=None):
    task = TaskHistory(task_id=task_id, compare_result=flag,result_detail=result_detail)
    try:
        db.session.add(task)
        db.session.commit()
        return True
    except Exception:
        logging.error(traceback.format_exc())
        db.session.rollback()
        return False


def get_history_obj_by_taskid(task_id):
    sql_obj = TaskHistory.query.filter(TaskHistory.task_id == task_id).all()
    return sql_obj
