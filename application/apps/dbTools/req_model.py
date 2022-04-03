# coding=utf-8
"""
File: req_model.py
Created on 2022/3/31 10:26
__author__= yangh
__remark__=
"""
import logging, traceback
from pydantic import BaseModel, Field, ValidationError, validator
from typing import List
from functools import wraps
from flask import request
from utils.result import Result
from enum import IntEnum
from apscheduler.triggers.cron import CronTrigger


def verify_request_json(model_name):
    def decorate(api_func):
        @wraps(api_func)
        def wrappers(*args, **kwargs):
            req = request.json
            if not req:
                return Result.fail(message="传参类型错误")
            try:
                model_name(**req)
            except ValidationError as e:
                logging.error(str(e).strip())
                traceback.print_exc()
                error_list = []
                for i in (e.errors()):
                    temp = {}
                    for k, v in i.items():
                        if isinstance(v, tuple):
                            temp[k] = v[0]
                        else:
                            temp[k] = v
                    error_list.append(temp)
                return Result.fail(message="参数错误", detail=error_list)
            return api_func()

        return wrappers

    return decorate


# compare_view相关模型
class CompareType(IntEnum):
    count = 0  # 数量对比
    struct = 1  # 结构对比


class CompareModel(BaseModel):
    """表记录、结构对比接口的请求模型/dbTools/compare/compareTable"""
    compareType: CompareType = Field(description="对比方式，0：比对表记录 1：比对表结构")
    source_configId: int = Field(..., description="源配置id")
    source_updateTime: int = Field(default=None, description="源更新时间")
    source_tableList: List[str] = Field(..., description="源比对表列表")
    source_pattern: str = Field(..., description="源表名模式")

    target_configId: int = Field(..., description="目标配置id")
    target_updateTime: int = Field(default=None, description="目的更新时间")
    target_tableList: List[str] = Field(..., description="目标比对表列表")
    target_pattern: str = Field(..., description="目标比对表列表")


class CompareSaveModel(CompareModel):
    """数据库表记录数、结构对比保存接口模型/dbTools/compare/compareSave"""
    task_name: str = Field(...)
    project_id: int = Field(...)
    task_time: str = Field(default=None)
    remarks: str = Field(default=None)

    @validator('task_time', pre=True)
    def check_datetime(cls, task_time):
        if isinstance(task_time, str) and task_time != None:
            try:
                CronTrigger.from_crontab(task_time)
            except Exception as e:
                logging.error(str(e).strip())
                raise ValueError(e)
            return task_time
        return task_time


# task_view相关模型

class TaskUpdateModel(BaseModel):
    """更新任务接口模型/dbTools/task/updateTask"""
    task_id: int = Field(...)
    task_name: str = Field(...)
    project_id: int = Field(...)
    task_time: str = Field(default=None)
    source_config_id: int = Field(...)
    target_config_id: int = Field(...)
    remarks: str = Field(default=None)

    @validator('task_time', pre=True)
    def check_datetime(cls, task_time):
        if isinstance(task_time, str) and task_time != None:
            try:
                CronTrigger.from_crontab(task_time)
            except Exception as e:
                logging.error(str(e).strip())
                raise ValueError(e)
            return task_time
        return task_time


# dbConfig_view相关模型
class Type(IntEnum):
    db = 0  # 连接数据库对比
    file = 1  # 上传结果文件对比


class ConfigAddModel(BaseModel):
    """新增数据库配置接口模型/dbTools/dbConfig/addConfig"""
    name: str = Field(...)
    host: str = Field(...)
    database: str = Field(...)
    user: str = Field(...)
    password: str = Field(...)
    type: Type = Field(description="对比方式，0：连接数据库对比 1：上传结果文件对比")


class ConfigUpdateModel(ConfigAddModel):
    """更新数据库配置接口模型/dbTools/dbConfig/updateConfig"""
    config_id: int = Field(...)


class ConfigTestModel(BaseModel):
    """测试数据库配置接口模型/dbTools/dbConfig/testConnect"""
    host: str = Field(...)
    database: str = Field(...)
    user: str = Field(...)
    password: str = Field(...)
