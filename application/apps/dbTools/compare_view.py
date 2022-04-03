# coding=utf-8
"""
File: compare_view.py
Created on 2022/3/28 10:40
__author__= yangh
__remark__=
"""
import uuid
import psycopg2
import psycopg2.errors
import logging, traceback
from apscheduler.triggers.cron import CronTrigger
from copy import deepcopy
from flask import request
from . import dbtools_blu
from utils.result import Result
from application import scheduler, db
from application.models import configuration
from application.models import projects
from application.models import tasks
from application.apps.dbTools.req_model import verify_request_json, CompareModel, CompareSaveModel
from application.services import compare_service
from application.services.job_service import compare_job
from datetime import datetime, timedelta


@dbtools_blu.route('/compare/compareTable', methods=["POST"])
@verify_request_json(CompareModel)
def compareTable():
    source_configId = request.json.get("source_configId")
    target_configId = request.json.get("target_configId")
    source_tableList = request.json.get("source_tableList")
    source_pattern = request.json.get("source_pattern")
    target_tableList = request.json.get("target_tableList")
    target_pattern = request.json.get("target_pattern")
    # 判断传入的配置id是否存在
    if not configuration.get_config_by_id(source_configId):
        return Result.fail(message="源配置id不存在")
    if not configuration.get_config_by_id(target_configId):
        return Result.fail(message="目标配置id不存在")
    # 获取配置信息
    db_config = compare_service.get_config_dictInfo_by_configId(source_configId, target_configId)
    # 连接数据库
    try:
        source_conn = psycopg2.connect(**db_config.get('cource_config'))
    except Exception as e:
        logging.error(traceback.format_exc())
        return Result.fail(message="源数据库连接失败", detail=str(e).strip())
    try:
        target_conn = psycopg2.connect(**db_config.get('target_config'))
    except Exception as e:
        logging.error(traceback.format_exc())
        return Result.fail(message="目标数据库连接失败", detail=str(e).strip())
    # 根据传入的表列表和表名模式，生成表的对应关系
    compare_data = None
    try:
        compare_data = compare_service.get_compare_data(source_pattern, source_tableList, target_pattern,
                                                        target_tableList)
    except Exception:
        traceback.print_exc()
        Result.fail(message="获取表对应关系异常")

    if request.json.get("compareType") == 0:  # 表记录对比
        source_updateTime = request.json.get("source_updateTime")
        target_updateTime = request.json.get("target_updateTime")
        now_time = datetime.now()
        source_old_time, target_old_time = None, None
        if source_updateTime:
            source_old_time = now_time - timedelta(hours=source_updateTime)
        if target_updateTime:
            target_old_time = now_time - timedelta(hours=target_updateTime)
        try:
            compare_result_list, compare_flag = compare_service.compare_table_records(compare_data_dict=compare_data,
                                                                                      now_time=now_time,
                                                                                      source_old_time=source_old_time,
                                                                                      target_old_time=target_old_time,
                                                                                      source_conn=source_conn,
                                                                                      target_conn=target_conn)
        except Exception:
            traceback.print_exc()
            return Result.fail(message="表记录数对比失败")
        response_dict = {"list": compare_result_list, "result": compare_flag}
        return Result.success(data=response_dict)
    elif request.json.get("compareType") == 1:  # 表结构对比
        try:
            result = compare_service.compare_table_structure(compare_data_dict=compare_data, source_conn=source_conn,
                                                             target_conn=target_conn)
        except Exception:
            traceback.print_exc()
            return Result.fail(message="表结构对比失败")
        return Result.success(data=result)


@dbtools_blu.route('/compare/compareSave', methods=["POST"])
@verify_request_json(CompareSaveModel)
def compareSave():
    """
    保存为任务
    @return:
    """
    origin_request_json = deepcopy(request.json)
    task_name = request.json.pop('task_name')
    compareType = request.json.pop('compareType')
    project_id = request.json.pop('project_id')
    task_time = request.json.pop('task_time')
    remarks = request.json.pop('remarks')
    source_configId = request.json.pop('source_configId')
    target_configId = request.json.pop('target_configId')
    task_detail_json = request.json
    # 判断项目id是否存在
    if not projects.is_project_exist(project_id):
        return Result.fail(message="项目id不存在")
    # 判断配置id是否存在
    if not configuration.get_config_by_id(source_configId):
        return Result.fail(message="源配置id不存在")
    if not configuration.get_config_by_id(target_configId):
        return Result.fail(message="目标配置id不存在")
    if tasks.task_is_repeat(task_name):
        logging.error(f"任务名称:[{task_name}]已存在")
        return Result.fail(message="任务名称已存在")
    job_id = str(uuid.uuid4()).split("-")[-1]
    if task_time == None:  # 不填写定时任务时间，不生成定时任务
        job_id = None
    insert_task_data = {
        "task_name": task_name,
        "compare_type": str(compareType),  # 数据库存的是枚举，要转成字符串才能存进去
        "project_id": project_id,
        "source_config_id": source_configId,
        "target_config_id": target_configId,
        "task_time": task_time,
        "remarks": remarks,
        "task_detail_json": task_detail_json,
        "job_id": job_id
    }
    task_id = tasks.add_compare_task(insert_task_data)
    if task_id:
        if task_time != None:  # 任务保存成功并且任务时间不为空时加入定时任务
            try:
                scheduler.add_job(func=compare_job, args=(origin_request_json, job_id, task_id), id=job_id,
                                  trigger=CronTrigger.from_crontab(task_time))
            except Exception:
                logging.error(traceback.format_exc())
                # 把原先添加成功的任务删除
                db.session.delete(tasks.get_task_info_by_id(task_id))
                db.session.execute(f"DELETE FROM apscheduler_jobs where id='{job_id}'")
                db.session.commit()
                return Result.fail(message="任务保存失败")
        return Result.success(message="任务保存成功")
    return Result.fail(message="任务保存失败")
