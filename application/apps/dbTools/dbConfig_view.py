# coding=utf-8
"""
File: dbConfig_view.py
Created on 2022/3/28 9:25
__author__= yangh
__remark__=
"""
import psycopg2
import psycopg2.errors
from . import dbtools_blu
from flask import request
from application.models.configuration import configuration_query_all, config_name_is_exist, add_config, \
    delete_config_by_id, get_config_by_id, is_config_name_repeat, update_config
from utils.result import Result
from utils.response_utils import ResponseUtils
from application.apps.dbTools.req_model import verify_request_json, ConfigAddModel, ConfigUpdateModel, ConfigTestModel


@dbtools_blu.route('/dbConfig/addConfig', methods=["POST"])
@verify_request_json(ConfigAddModel)
def addConfig():
    """
    @return:
    """
    name = request.json.get('name')
    if config_name_is_exist(name):
        return Result.fail(message="该配置名称已存在")
    config_type = request.json.get('type')
    if config_type == 1:  # 比对类型为上传结果文件
        return Result.fail(message="暂不支持上传文件对比")
    else:
        host = request.json.get('host')
        database = request.json.get('database')
        schema = request.json.get('schema')
        user = request.json.get('user')
        password = request.json.get('password')
        if (add_config(name, host, database, schema, user, password, config_type)):
            return Result.success(message="配置保存成功")
        return Result.fail(message="配置保存失败")


@dbtools_blu.route('/dbConfig/deleteConfig', methods=["POST"])
def deleteConfig():
    """
    删除源目的库配置
    @return:
    """
    config_id = request.json.get('config_id')
    if delete_config_by_id(config_id):
        return Result.success(message="配置删除成功")
    return Result.fail(message="配置删除失败")


@dbtools_blu.route('/dbConfig/updateConfig', methods=['POST'])
@verify_request_json(ConfigUpdateModel)
def updateConfig():
    """
    修改源目的库配置
    @return:
    """
    config_id = request.json.get("config_id")
    name = request.json.get('name')
    config_type = request.json.get('type')
    if not get_config_by_id(config_id):
        return Result.fail(message="配置记录不存在")
    if is_config_name_repeat(config_id, name):
        return Result.fail(message="该配置名称已存在")
    if config_type == 1:  # 比对类型为上传结果文件
        return Result.fail(message="暂不支持上传文件对比")
    else:
        host = request.json.get('host')
        database = request.json.get('database')
        schema = request.json.get('schema')
        user = request.json.get('user')
        password = request.json.get('password')
        if (update_config(config_id, name, host, database, schema, user, password, config_type)):
            return Result.success(message="配置修改成功")
        return Result.fail(message="配置修改失败")


@dbtools_blu.route('/dbConfig/getConfigInfo', methods=["GET"])
def getConfigInfo():
    """
    查询源目的库配置信息
    @return:
    """
    res = configuration_query_all()
    return Result.success(data=ResponseUtils.get_dict_sql_result(res))


@dbtools_blu.route('/dbConfig/testConnect', methods=["POST"])
@verify_request_json(ConfigTestModel)
def testConnect():
    """
    测试数据库连接，连接正常才能进行比对
    @return:
    """
    try:
        psycopg2.connect(host=request.json.get('host'), port=5432, database=request.json.get('database'),
                         user=request.json.get('user'), password=request.json.get('password'))
    except Exception as e:
        print(e)
        return Result.fail(message="数据库连接失败", detail=str(e).strip())
    return Result.success(message="数据库连接成功")
