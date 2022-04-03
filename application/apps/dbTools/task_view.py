# coding=utf-8
"""
File: task_view.py
Created on 2022/3/28 10:20
__author__= yangh
__remark__=
"""
import logging, traceback
import uuid
from apscheduler.triggers.cron import CronTrigger
from . import dbtools_blu
from flask import request
from utils.result import Result
from utils.response_utils import ResponseUtils
from application import scheduler, db
from application.models.projects import Projects, is_project_exist
from application.models.configuration import get_config_by_id
from application.models.tasks import is_task_name_repeat, get_compare_task_list, delete_task_and_job, \
    get_task_info_by_id
from application.models.task_history import get_task_run_history_last5, get_task_list
from application.models.apschedler_jobs import get_job_obj_by_id
from application.services.job_service import compare_job
from application.apps.dbTools.req_model import verify_request_json, TaskUpdateModel


@dbtools_blu.route('/taskHistory/getRunList', methods=["get"])
def getRunList():
    """
    查询任务列表
    """
    try:
        sql_result = get_task_list()
        return Result.success(data=sql_result)
    except Exception:
        Result.fail()


@dbtools_blu.route('/taskHistory/getLast5', methods=["get"])
def getLast5():
    """
    查询最近5次运行历史
    """
    task_id = request.args.to_dict().get('taskId')
    sql_result = get_task_run_history_last5(task_id)
    return Result.success(data=sql_result)


@dbtools_blu.route('/task/getTaskList', methods=["get"])
def task_getTaskList():
    """
    查询对比任务列表
    """
    try:
        result = get_compare_task_list()
        return Result.success(data=result)
    except Exception:
        traceback.print_exc()
        return Result.fail()


@dbtools_blu.route('/projects', methods=["GET"])
def get_projects():
    projects_data = Projects.query.all()
    try:
        data = ResponseUtils.get_dict_sql_result(projects_data)
    except Exception as e:
        print(e)
        return Result.fail()
    return Result.success(data=data)


@dbtools_blu.route('/task/updateTask', methods=["POST"])
@verify_request_json(TaskUpdateModel)
def updateTask():
    task_name = request.json.get('task_name')
    project_id = request.json.get('project_id')
    task_id = request.json.get("task_id")
    task_time = request.json.get("task_time")
    source_config_id = request.json.get('source_config_id')
    target_config_id = request.json.get('target_config_id')
    remarks = request.json.get('remarks')
    project_obj = is_project_exist(project_id)
    if not project_obj:
        return Result.fail(message="项目不存在")
    task_obj = get_task_info_by_id(task_id)
    if not task_obj:
        return Result.fail(message="任务不存在")
    task_compare_type = int(task_obj.compare_type)  # 数据库里存的是枚举，字符串类型的。需要重新转回int
    job_id = task_obj.job_id  # 定时任务id
    if is_task_name_repeat(task_id, task_name):
        return Result.fail(message="任务名称已存在")
    if not get_config_by_id(source_config_id):
        return Result.fail(message="源配置不存在")
    if not get_config_by_id(target_config_id):
        return Result.fail(message="目标配置不存在")
    # 更新相关字段
    task_obj.source_config_id = source_config_id
    task_obj.target_config_id = target_config_id
    task_obj.task_name = task_name
    task_obj.task_time = task_time
    task_obj.project_id = project_id
    task_obj.remarks = remarks
    job_obj = get_job_obj_by_id(job_id)  # 定时任务表的任务对象模型
    if task_time == None:  # 任务时间为空时需要将原来的定时任务删除
        try:
            if job_obj:
                db.session.delete(job_obj)  # 删除定时任务
            db.session.commit()
        except Exception:
            traceback.print_exc()
            db.session.rollback()
            return Result.fail(message="任务修改失败")
    else:  # 需要修改定时任务
        # 重新组装定时任务的请求json
        job_json = task_obj.task_detail_json
        job_json['source_configId'] = source_config_id
        job_json['target_configId'] = target_config_id
        job_json['task_time'] = task_time
        job_json['compareType'] = task_compare_type
        try:
            if not job_obj:  # 定时任务表中不存在该任务
                if job_id == None:  # 任务表中该字段为null时需要重新生成定时任务id
                    job_id = str(uuid.uuid4()).split("-")[-1]
                    task_obj.job_id = job_id
                scheduler.add_job(func=compare_job, args=(job_json, job_id, task_id), id=job_id,
                                  trigger=CronTrigger.from_crontab(task_time))
            else:
                scheduler.modify_job(id=job_id, args=(job_json, job_id, task_id))  # 修改定时任务参数
                scheduler._scheduler.reschedule_job(job_id=job_id,
                                                    trigger=CronTrigger.from_crontab(task_time))  # 修改定时任务运行时间
            db.session.commit()
        except Exception:
            traceback.print_exc()
            db.session.rollback()
            return Result.success(message="任务修改失败")
    return Result.success(message="任务修改成功")


@dbtools_blu.route('/task/deleteTask', methods=["POST"])
def task_delete():
    # 判断taskid是否存在
    task_id = request.json.get("task_id")
    task_obj = get_task_info_by_id(task_id)
    if not task_obj:
        return Result.fail(message="任务不存在")
    if delete_task_and_job(task_id):
        return Result.success(message="任务删除成功")
    return Result.fail(message="任务删除失败")
