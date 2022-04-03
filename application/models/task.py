# coding=utf-8
from datetime import datetime
from application import db
from application.models.result import get_result_obj


class Task(db.Model):
    """已保存的任务列表"""
    __tablename__ = "task_list"
    id = db.Column(db.Integer, primary_key=True, comment="主键ID")
    task_name = db.Column(db.String(64), unique=True, comment="任务名称")
    task_type = db.Column(db.Integer, comment="任务类型，0：表记录对比 1：表结构对比")
    create_time = db.Column(db.DateTime, default=datetime.now, comment="创建时间")
    update_time = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    project_id = db.Column(db.Integer,db.ForeignKey('projects.id'),comment="所属项目id") #project_id关联projects表
    timing_day = db.Column(db.String(64), comment="定时_天")
    timing_hour = db.Column(db.Integer, comment="定时_时")
    timing_minute = db.Column(db.Integer, comment="定时_分")
    remarks = db.Column(db.String(64), comment="备注")

    config_id = db.Column(db.Integer, comment="对应配置id")
    source_host = db.Column(db.String(64), comment="源host")
    source_port = db.Column(db.Integer, comment="源port")
    source_database = db.Column(db.String(64), comment="源database")
    source_schema = db.Column(db.String(64), comment="源schema")
    source_user = db.Column(db.String(64), comment="源user")
    source_password = db.Column(db.String(64), comment="源password")

    target_type = db.Column(db.Integer, comment="目的数据库比对类型 0：数据库 1：结果文件")
    target_host = db.Column(db.String(64), comment="目的host")
    target_port = db.Column(db.Integer, comment="目的port")
    target_database = db.Column(db.String(64), comment="目的database")
    target_schema = db.Column(db.String(64), comment="目的schema")
    target_user = db.Column(db.String(64), comment="目的user")
    target_password = db.Column(db.String(64), comment="目的password")
    target_file_name = db.Column(db.String(255), comment="目的数据库文件名称")
    target_file_path = db.Column(db.String(255), comment="目的数据库文件保存路径")

    source_insert_start = db.Column(db.DateTime, comment="源insert起始时间")
    source_insert_end = db.Column(db.DateTime, comment="源insert截至时间")
    source_update_start = db.Column(db.DateTime, comment="源update起始时间")
    source_update_end = db.Column(db.DateTime, comment="源update截至时间")
    target_insert_start = db.Column(db.DateTime, comment="目的insert起始时间")
    target_insert_end = db.Column(db.DateTime, comment="目的insert截至时间")
    target_update_start = db.Column(db.DateTime, comment="目的update起始时间")
    target_update_end = db.Column(db.DateTime, comment="目的update截至时间")
    tables_list = db.Column(db.JSON, comment="比对列表(表、视图、物化视图)")

    except_field_one = db.Column(db.String(255), comment="预留字段1")
    except_field_two = db.Column(db.String(255), comment="预留字段2")


def task_is_repeat(task_name):
    """
    判断任务名称是否重复
    @param task_name:
    @return:
    """
    if Task.query.filter_by(task_name=task_name).first():
        return True
    return False


def compare_task_add(compare_dict, task_name, task_type, project_id, timing_day, timing_hour, timing_minute, remarks,
                     config_id, source_host, source_database, source_schema, source_user, source_password, target_type,
                     target_host, target_database, target_schema, target_user, target_password, target_file_name,
                     target_file_path, source_insert_start, source_insert_end, source_update_start, source_update_end,
                     target_insert_start, target_insert_end, target_update_start, target_update_end, tables_list):
    """
    对比结束后，添加此任务及本次结果
    @param compare_dict: 对比结果
    @param task_name: 任务名称
    @param task_type: 任务类型 0：表记录 1：表结构
    @param project_id: 所属项目id
    @param timing_day: 定期周期
    @param timing_hour:
    @param timing_minute:
    @param remarks: 备注
    @param config_id: 对应配置id 0：不限，手动输入 其他：读取配置表
    @param source_host:
    @param source_database:
    @param source_schema:
    @param source_user:
    @param source_password:
    @param target_type:
    @param target_host:
    @param target_database:
    @param target_schema:
    @param target_user:
    @param target_password:
    @param target_file_name:
    @param target_file_path:
    @param source_insert_start:
    @param source_insert_end:
    @param source_update_start:
    @param source_update_end:
    @param target_insert_start:
    @param target_insert_end:
    @param target_update_start:
    @param target_update_end:
    @param tables_list: 要对比的表
    @return:
    """
    task = Task(task_name=task_name, task_type=task_type, project_id=project_id, timing_day=timing_day,
                timing_hour=timing_hour, timing_minute=timing_minute, remarks=remarks, config_id=config_id,
                source_host=source_host, source_port=5432, source_database=source_database, source_schema=source_schema,
                source_user=source_user, source_password=source_password, target_type=target_type,
                target_host=target_host, target_port=5432, target_database=target_database,
                target_schema=target_schema, target_user=target_user, target_password=target_password,
                target_file_name=target_file_name, target_file_path=target_file_path,
                source_insert_start=source_insert_start, source_insert_end=source_insert_end,
                source_update_start=source_update_start, source_update_end=source_update_end,
                target_insert_start=target_insert_start, target_insert_end=target_insert_end,
                target_update_start=target_update_start, target_update_end=target_update_end,
                tables_list=tables_list)
    try:
        db.session.add(task)
        db.session.flush()
        task_id = task.id
        result = get_result_obj(task_id, compare_dict)
        db.session.add(result)
        db.session.commit()
    except Exception as e:
        print(e)
        db.session.rollback()
        return False
    return True


def task_is_modified(task_id, task_name):
    """
    判断任务名称是否被修改过
    @param task_id: 任务id主键
    @param task_name: 任务名称
    @return:
    """
    task = Task.query.filter_by(id=task_id).first()
    if task.task_name == task_name:
        return False
    return True


def compare_task_update(task_id, task_name, project_id, timing_day, timing_hour, timing_minute,
                        remarks, config_id, source_host, source_database, source_schema, source_user,
                        source_password):
    task = Task.query.filter_by(id=task_id).first()
    if not task:
        return False

    task.task_name = task_name
    task.project_id = project_id
    task.timing_day = timing_day
    task.timing_hour = timing_hour
    task.timing_minute = timing_minute
    task.remarks = remarks
    task.config_id = config_id
    task.source_host = source_host
    task.source_database = source_database
    task.source_schema = source_schema
    task.source_user = source_user
    task.source_password = source_password

    try:
        db.session.commit()
    except Exception as e:
        print(e)
        db.session.rollback()
        return False
    return True


def compare_task_delete():
    pass


def compare_task_query():
    pass



