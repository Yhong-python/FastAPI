# coding=utf-8
"""
File: job_service.py
Created on 2022/3/21 17:20
__author__= yangh
__remark__=
"""
from datetime import datetime, timedelta
import psycopg2
import traceback, logging
from application import scheduler
from application.services import compare_service
from application.models.task_history import insert_task


class JobService:
    @staticmethod
    def compare_func(request_json):
        source_configId = request_json.get("source_configId")
        target_configId = request_json.get("target_configId")
        source_tableList = request_json.get("source_tableList")
        source_pattern = request_json.get("source_pattern")
        target_tableList = request_json.get("target_tableList")
        target_pattern = request_json.get("target_pattern")
        # 获取配置信息
        db_config = compare_service.get_config_dictInfo_by_configId(source_configId, target_configId)
        # 连接数据库
        try:
            source_conn = psycopg2.connect(**db_config.get('cource_config'))
        except Exception:
            traceback.print_exc()
            logging.error(traceback.format_exc())
            return -1
        try:
            target_conn = psycopg2.connect(**db_config.get('target_config'))
        except Exception:
            traceback.print_exc()
            logging.error(traceback.format_exc())
            return -1
        # 根据传入的表列表和表名模式，生成表的对应关系
        try:
            compare_data = compare_service.get_compare_data(source_pattern, source_tableList, target_pattern,
                                                            target_tableList)
        except Exception:
            traceback.print_exc()
            logging.error(traceback.format_exc())
            return -1
        if request_json.get("compareType") == 0:  # 表记录对比
            source_updateTime = request_json.get("source_updateTime")
            target_updateTime = request_json.get("target_updateTime")
            now_time = datetime.now()
            source_old_time, target_old_time = None, None
            if source_updateTime:
                source_old_time = now_time - timedelta(hours=source_updateTime)
            if target_updateTime:
                target_old_time = now_time - timedelta(hours=target_updateTime)
            try:
                compare_result_list, compare_flag = compare_service.compare_table_records(
                    compare_data_dict=compare_data,
                    now_time=now_time,
                    source_old_time=source_old_time,
                    target_old_time=target_old_time,
                    source_conn=source_conn,
                    target_conn=target_conn)
                return compare_result_list,compare_flag
            except Exception:
                traceback.print_exc()
                return -1
        elif request_json.get("compareType") == 1:  # 表结构对比
            try:
                compare_result = compare_service.compare_table_structure(compare_data_dict=compare_data,
                                                                         source_conn=source_conn,
                                                                         target_conn=target_conn)
                return compare_result
            except Exception:
                traceback.print_exc()
                return -1


def compare_job(request_json, id, task_id):
    logging.info("************定时任务开始************")
    # 获取对比任务
    with scheduler.app.app_context():
        try:
            result = JobService.compare_func(request_json)
        except Exception:
            traceback.print_exc()
            logging.error("定时任务异常")
            logging.error(traceback.format_exc())
            flag = -1  # 任务为异常
            insert_task(task_id=task_id, flag=flag)
            print(f"任务:{task_id}的定时任务运行结果：{flag}")
        else:
            # 结果保存到task_history
            if request_json.get("compareType")==0:
                result_json=result[0]
                flag=result[1]
            elif request_json.get("compareType")==1:
                result_json=result
                flag=result.get('result')
            insert_task(task_id=task_id, flag=flag,result_detail=result_json)
            print(f"任务:{task_id}的定时任务运行结果：{flag}")
    logging.info("************定时任务结束************")
