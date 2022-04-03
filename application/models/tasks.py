# coding=utf-8
import traceback,logging
from datetime import datetime
from application import db
from enum import IntEnum
from application import scheduler
from application.models import apschedler_jobs
from application.models import task_history
from sqlalchemy.orm.attributes import flag_modified


class TaskType(IntEnum):
    count = 0  # 表记录对比
    struct = 1  # 表结构对比


class Tasks(db.Model):
    """已保存的任务列表"""
    __tablename__ = "tasks"

    id = db.Column(db.Integer, autoincrement=True, primary_key=True, comment="主键ID")
    task_name = db.Column(db.String(64), nullable=False, unique=True, comment="任务名称")
    compare_type = db.Column(db.Enum('0', '1'), server_default='0', nullable=False, comment="任务类型，0：表记录对比 1：表结构对比")
    project_id = db.Column(db.Integer, db.ForeignKey('projects.id', ondelete='SET NULL'),
                           comment="所属项目id")  # project_id关联projects表
    task_time = db.Column(db.String(64), comment="定时任务执行时间（Crontab语法）")
    remarks = db.Column(db.String(500), comment="备注")
    source_config_id = db.Column(db.Integer, db.ForeignKey('configuration.id', ondelete='SET NULL'), comment="源数据库配置id")
    target_config_id = db.Column(db.Integer, db.ForeignKey('configuration.id', ondelete='SET NULL'),
                                 comment="目标数据库配置id")
    task_detail_json = db.Column(db.JSON, comment="")
    job_id = db.Column(db.String(64), comment="定时任务id")
    create_time = db.Column(db.DateTime, default=datetime.now, comment="创建时间")
    update_time = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    tasks = db.relationship('TaskHistory', backref='tasks', lazy="dynamic")  # 给TaskHistory表做外键


def get_task_info_by_id(task_id):
    """
    判断配置id是否存在
    @param task_id: 主键id
    @return:
    """
    result = Tasks.query.filter(Tasks.id == task_id).first()
    return result


def task_is_repeat(task_name):
    """
    判断任务名称是否重复
    @param task_name:
    @return:
    """
    if Tasks.query.filter_by(task_name=task_name).first():
        return True
    return False


def add_compare_task(json_data):
    """
    对比任务和该任务执行记录入库
    @param json_data:
    @return:
    """
    try:
        # result = json_data.pop('result')
        task = Tasks(**json_data)
        db.session.add(task)
        db.session.flush()
        task_id = task.id
        # history = get_history_obj(task_id, result)
        # db.session.add(history)
        db.session.commit()
    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        return False
    return task_id


def get_compare_task_list():
    """
    获取对比任务列表
    @return:
    """
    # outerjoin等于left join
    # db_model_result = Tasks.query.\
    #     outerjoin(Projects, Projects.id == Tasks.project_id).\
    #     outerjoin(Configuration,Configuration.id == Tasks.source_config_id). \
    #     outerjoin(Configuration, Configuration.id == Tasks.target_config_id).\
    #     with_entities(Projects.name, Tasks.task_name, Tasks.task_time, Configuration.config_name, Tasks.remarks,
    #                   Tasks.task_detail_json).order_by(Projects.name.asc()).all()
    db_result = db.session.execute(
        """
        SELECT p.`name`, t.task_name,t.task_time,c.id,c.`name`,c.host,c.port,c.database,c.schema,c.user,c.password,c2.id,c2.`name`,c2.host,c2.port,c2.database,c2.schema,c2.user,c2.password,t.remarks,p.id
        FROM tasks t
        LEFT JOIN projects p ON t.project_id=p.id
        LEFT JOIN configuration c ON c.id=t.source_config_id
        LEFT JOIN configuration c2 ON c2.id=t.target_config_id
        ORDER BY p.`name`
        """)
    result = []
    for i in list(db_result):
        config = {
            "source_config": {
            },
            "target_config": {

            }
        }
        config['source_config']['id'] = i[3]
        config['source_config']['name'] = i[4]
        config['source_config']['host'] = i[5]
        config['source_config']['port'] = i[6]
        config['source_config']['database'] = i[7]
        config['source_config']['schema'] = i[8]
        config['source_config']['user'] = i[9]
        config['source_config']['password'] = i[10]

        config['target_config']['id'] = i[11]
        config['target_config']['name'] = i[12]
        config['target_config']['host'] = i[13]
        config['target_config']['port'] = i[14]
        config['target_config']['database'] = i[15]
        config['target_config']['schema'] = i[16]
        config['target_config']['user'] = i[17]
        config['target_config']['password'] = i[18]
        result_dict = {
            "project_name": i[0],
            "project_id": i[20],
            "task_name": i[1],
            "task_time": i[2],
            "remarks": i[19],
            "config": config,
        }
        result.append(result_dict)
    result=sorted(result,key=lambda x:x['task_name'])
    return result


def is_task_name_repeat(task_id, task_name):
    result = Tasks.query.filter(Tasks.id != task_id, Tasks.task_name == task_name).all()
    if result:
        return True
    return False


def update_compare_task(json_dict):
    source_config_id = json_dict.get('source_config_id')
    target_config_id = json_dict.get('target_config_id')
    task_id = json_dict.get('task_id')
    task_model_obj = Tasks.query.filter(Tasks.id == task_id).first()
    task_model_obj.task_name = json_dict.get('task_name')
    task_model_obj.project_id = json_dict.get('project_id')
    task_model_obj.task_time = json_dict.get('task_time')
    task_model_obj.remarks = json_dict.get('remarks')
    task_model_obj.source_config_id = source_config_id
    task_model_obj.target_config_id = target_config_id
    try:
        db.session.commit()
        return True
    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        return False


def delete_task_and_job(task_id):
    """
    删除配置
    @param task_id: 任务id
    @return:
    """
    task_info = get_task_info_by_id(task_id)
    # 获取定时任务id
    job_id = task_info.job_id
    job_obj = apschedler_jobs.get_job_obj_by_id(job_id)
    task_historys = task_history.get_history_obj_by_taskid(task_id)
    if not task_info:
        return False
    try:
        # 先删除任务历史
        for i in task_historys:
            db.session.delete(i)
        db.session.delete(task_info)  # 删除任务表
        if job_obj:
            db.session.delete(job_obj)  # 删除定时任务
            # scheduler.remove_job(job_id)   #删除定时任务
        db.session.commit()
    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        return False
    return True
