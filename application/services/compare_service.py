# coding=utf-8
"""
File: compare_service.py
Created on 2022/3/18 9:56
__author__= yangh
__remark__=
"""
import re
import traceback
import logging
import psycopg2
import psycopg2.errors
from psycopg2.errorcodes import UNDEFINED_TABLE
from application.models import configuration
from utils.response_utils import ResponseUtils


def connect_database(request_json: dict):
    source_connect_info = {
        "host": request_json.get('source_host'),
        "port": 5432,
        "database": request_json.get('source_database'),
        "user": request_json.get('source_user'),
        "password": request_json.get('source_password')
    }
    target_connect_info = {
        "host": request_json.get('target_host'),
        "port": 5432,
        "database": request_json.get('target_database'),
        "user": request_json.get('target_user'),
        "password": request_json.get('target_password')
    }
    source_conn = psycopg2.connect(**source_connect_info)
    target_conn = psycopg2.connect(**target_connect_info)
    return source_conn, target_conn


def get_table_fields_info(db_conn, table_name, db_type=1):
    """
    获取表的字段信息
    @param table_list:数据库连接游标
    @param table_list:表名列表
    @param db_type:表类型，1为源表，2为目标表
    @return:
    """
    column_type = None
    is_null = None
    if db_type == 1:
        column_type = "source_column_type"
        is_null = "source_is_null"
    elif db_type == 2:
        column_type = "target_column_type"
        is_null = "target_is_null"
    db_conn_cursor = db_conn.cursor()
    result = {}
    get_table_fields_info_sql = f"SELECT aa.schema_name,aa.table_name,aa.column_name,aa.column_type,aa.column_length,aa.is_null,f.description FROM(SELECT d.nspname AS SCHEMA_NAME,A.relname AS TABLE_NAME,b.attname AS COLUMN_NAME,C.typname AS column_type,b.attlen AS column_length,b.attnotnull AS is_null,b.attrelid,b.attnum FROM pg_class A,pg_attribute b,pg_type C,pg_namespace d WHERE A.relkind = 'r' AND A.relnamespace = d.oid AND A.oid = b.attrelid AND C.oid = b.atttypid AND b.attnum > 0 AND NOT b.attisdropped AND a.relname='{table_name}') aa LEFT JOIN pg_description f ON aa.attrelid = f.objoid AND aa.attnum = f.objsubid ORDER BY aa.table_name;"
    try:
        db_conn_cursor.execute(get_table_fields_info_sql)
        rows = db_conn_cursor.fetchall()
        # 只需要字段名、字段类型、是否为空三个
        # temp_dict = {}
        for i in rows:
            result[i[2]] = {column_type: i[3], is_null: i[5]}
    except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
        logging.error(traceback.format_exc())
        # 该表不存在时，记录数返回None，并回滚事务
        db_conn.rollback()
        return False
    except Exception as e:
        logging.error(traceback.format_exc())
        db_conn.rollback()
        return False
    return result


def get_table_constraints_info(db_conn, table_name, db_type=1):
    """
    获取表的约束信息
    @param db_type:表类型，1为源表，2为目标表
    @return:
    """
    constraint_type = None
    if db_type == 1:
        constraint_type = "source_constraint_type"
    elif db_type == 2:
        constraint_type = "target_constraint_type"
    db_conn_cursor = db_conn.cursor()
    result = {}
    # 键值对常量
    constant = {
        "u": "unique",
        "p": "primary key",
        'f': "foreign key"
    }
    get_table_constraints_info_sql = f" select a.relname as tablename,'table constraint' ,c.conname as constraint_name,c.contype::varchar as constraint_type from pg_class a,pg_constraint c,pg_namespace d where a.relkind in ('r','p') and a.relnamespace=d.oid and a.oid=c.conrelid and a.relname='{table_name}' union all select a.relname as tablename,'column constraint',b.attname as column_name, 'not null' from pg_class a,pg_attribute b,pg_namespace c where a.relkind in ('r','p') and a.relnamespace=c.oid and b.attnum>0 and a.oid=b.attrelid and a.relname='{table_name}' and b.attnotnull;"
    try:
        db_conn_cursor.execute(get_table_constraints_info_sql)
        rows = db_conn_cursor.fetchall()
        for i in rows:
            result[i[2]] = {constraint_type: constant[i[3]] if i[3] in constant else i[3]}  # 当类型是u/p/f时进行转换
    except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
        logging.error(traceback.format_exc())
        # 该表不存在时，记录数返回None，并回滚事务
        db_conn.rollback()
        return False
    except Exception as e:
        logging.error(traceback.format_exc())
        db_conn.rollback()
        return False
    return result


def get_table_indexs_info(db_conn, table_name, db_type=1):
    """
    获取表的索引信息
    @param db_conn: 数据库连接对象
    @param table_list:表名列表
    @param db_type:表类型，1为源表，2为目标表
    @return:
    """
    index_name, index_columns, index_pk, index_unique = None, None, None, None
    if db_type == 1:
        index_columns = "source_index_columns"
        # index_null = "source_index_null"
        index_pk = "source_index_pk"
        index_unique = "source_index_unique"
    elif db_type == 2:
        index_columns = "target_index_columns"
        # index_null = "target_index_null"
        index_pk = "target_index_pk"
        index_unique = "target_index_unique"
    db_conn_cursor = db_conn.cursor()
    result = {}
    get_table_index_info_sql = f"select n.nspname as schemaname,a.relname as tablename,i.relname as indexname,m.amname as indextype,array(select pg_get_indexdef(idx.indexrelid,k+1,true) from generate_subscripts(idx.indkey,1) as k order by k) as index_columns,idx.indisunique as is_unique,idx.indisprimary as is_pk from pg_class a,pg_class i,pg_index idx,pg_namespace n,pg_am m where a.relnamespace=n.oid and a.relkind in ('r','p') and i.relkind='i' and a.oid=idx.indrelid and i.oid=idx.indexrelid and i.relam=m.oid and a.relname='{table_name}';"
    try:
        db_conn_cursor.execute(get_table_index_info_sql)
        rows = db_conn_cursor.fetchall()
        for i in rows:
            # i[2]是索引名
            # result[i[2]] = {index_columns: ','.join(i[4]), index_null: i[6], index_unique: i[5]}
            result[i[2]] = {index_columns: ','.join(i[4]), index_pk: i[6], index_unique: i[5]}
    except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
        logging.error(traceback.print_exc())
        logging.error(traceback.format_exc())
        # 该表不存在时，记录数返回None，并回滚事务
        db_conn.rollback()
        return False
    except Exception as e:
        logging.error(traceback.print_exc())
        logging.error(traceback.format_exc())
        db_conn.rollback()
        return False
    return result


def compare_table_fields_info(source_filds_dict: dict, target_filds_dict: dict):
    """
    表字段对比
    @param source_filds_dict: 源表字段信息
    @param target_filds_dict: 目标表字段信息
    @param table_list: 对比的表列表
    @return: 对比
    """
    table_same_flag = 1
    fields_info = {}
    all_keys = source_filds_dict.keys() | target_filds_dict.keys()
    common_keys = source_filds_dict.keys() & target_filds_dict.keys()
    if len(all_keys) == 0 and len(common_keys) == 0:  # 没有key时直接返回空字典
        table_same_flag = 0
        # fields_info=None
        return fields_info, table_same_flag
    # 共有的key
    for key in common_keys:
        temp_dict = dict(**source_filds_dict[key], **target_filds_dict[key])
        # 校验内容是否相同
        if temp_dict.get("source_column_type") == temp_dict.get('target_column_type') and temp_dict.get(
                'source_is_null') == \
                temp_dict.get('target_is_null'):
            temp_dict.update({"is_same": 1})
        else:
            temp_dict.update({"is_same": 0})
            table_same_flag = 0
        fields_info[key] = temp_dict
    # 其余key
    diff_keys = all_keys - common_keys
    for key in diff_keys:
        table_same_flag = 0
        if key in source_filds_dict:  # 多的key在源表
            fields_info[key] = source_filds_dict[key]
            fields_info[key].update({"target_column_type": None, "target_is_null": None, "is_same": 0})
        elif key in target_filds_dict:  # 多的key在目标表
            fields_info[key] = target_filds_dict[key]
            fields_info[key].update({"source_column_type": None, "sourceis_null": None, "is_same": 0})
    return fields_info, table_same_flag


def compare_table_constraint(source_constraint_dict: dict, target_constraint_dict: dict):
    constraint_same_flag = 1
    constraints_info = {}
    all_keys = source_constraint_dict.keys() | target_constraint_dict.keys()
    common_keys = source_constraint_dict.keys() & target_constraint_dict.keys()
    if len(all_keys) == 0 and len(common_keys) == 0:
        constraint_same_flag = 0
        # constraints_info=None
        return constraints_info, constraint_same_flag
    # 都有的key
    for key in common_keys:
        temp_dict = dict(**source_constraint_dict[key], **target_constraint_dict[key])
        # 校验内容是否相同
        if temp_dict["source_constraint_type"] == temp_dict['target_constraint_type']:
            temp_dict.update({"is_same": 1})
        else:
            temp_dict.update({"is_same": 0})
            constraint_same_flag = 0
        constraints_info[key] = temp_dict
    # 其余key
    diff_keys = all_keys - common_keys
    for key in diff_keys:
        constraint_same_flag = 0
        if key in source_constraint_dict:  # 多的key在源表
            constraints_info[key] = source_constraint_dict[key]
            constraints_info[key].update({"target_constraint_type": None, "is_same": 0})
        elif key in target_constraint_dict:  # 多的key在目标表
            constraints_info[key] = target_constraint_dict[key]
            constraints_info[key].update({"source_constraint_type": None, "is_same": 0})
    return constraints_info, constraint_same_flag


def compare_table_index(source_index_dict: dict, target_index_dict: dict):
    index_same_flag = 1
    index_info = {}
    all_keys = source_index_dict.keys() | target_index_dict.keys()
    common_keys = source_index_dict.keys() & target_index_dict.keys()
    if len(all_keys) == 0 and len(common_keys) == 0:
        index_same_flag = 0
        # index_info=None
        return index_info, index_same_flag
    # 都有的key
    for key in common_keys:
        temp_dict = dict(**source_index_dict[key], **target_index_dict[key])
        # 校验内容是否相同
        if temp_dict.get("source_index_columns") == temp_dict.get('target_index_columns') and temp_dict.get(
                "source_index_null") == temp_dict.get('target_index_null') and temp_dict.get(
            "source_index_unique") == temp_dict.get('target_index_unique'):
            temp_dict.update({"is_same": 1})
        else:
            temp_dict.update({"is_same": 0})
            index_same_flag = 0
        index_info[key] = temp_dict
    # 其余key
    diff_keys = all_keys - common_keys
    for key in diff_keys:
        index_same_flag = 0
        if key in source_index_dict:  # 多的key在源表
            index_info[key] = source_index_dict[key]
            index_info[key].update(
                {"target_index_columns": None, "target_index_null": None, "target_index_unique": None, "is_same": 0})
        elif key in target_index_dict:  # 多的key在目标表
            index_info[key] = target_index_dict[key]
            index_info[key].update(
                {"source_index_columns": None, "source_index_null": None, "source_index_unique": None, "is_same": 0})
    return index_info, index_same_flag


def get_config_dictInfo_by_configId(source_configId, target_configId):
    """
    获取源和目的的数据库配置信息
    @param source_configId:
    @param target_configId:
    @return:
    """
    config_dict_info = {
        "cource_config": {},
        "target_config": {}
    }
    source_db_result = configuration.get_config_by_id(source_configId)
    target_db_result = configuration.get_config_by_id(target_configId)
    r1 = ResponseUtils.get_dict_sql_result(source_db_result)
    r2 = ResponseUtils.get_dict_sql_result(target_db_result)
    config_dict_info['cource_config']['host'] = r1.get('host')
    config_dict_info['cource_config']['database'] = r1.get('database')
    # config_dict_info['cource_config']['schema']=r1.get('schema')
    config_dict_info['cource_config']['user'] = r1.get('user')
    config_dict_info['cource_config']['password'] = r1.get('password')
    config_dict_info['cource_config']['port'] = r1.get('port')
    config_dict_info['target_config']['host'] = r2.get('host')
    config_dict_info['target_config']['database'] = r2.get('database')
    # config_dict_info['target_config']['schema']=r2.get('schema')
    config_dict_info['target_config']['user'] = r2.get('user')
    config_dict_info['target_config']['password'] = r2.get('password')
    config_dict_info['target_config']['port'] = r2.get('port')
    return config_dict_info


def get_pattern(input_str: str):
    """
    根据传入的表名模式字段将该值转成正则
    @param input_str: 表名模式
    @return: 正则表达式
    """
    pattern = input_str
    if "*" in input_str:
        pattern = input_str.replace('*', "(.*)?")
    if "#" in pattern:
        pattern = pattern.replace('#', ".*?")
    return pattern


def get_compare_data(source_pattern, source_tableList, target_pattern, target_tableList):
    # 把规则转化为正则表达式
    source_pattern = get_pattern(source_pattern)
    traget_pattern = get_pattern(target_pattern)
    # 先找出源表中符合规则的表
    source_dict = {}  # 源表中符合表名模式的表
    target_dict = {}  # 目标表中符合表名模式的表
    pattern_dict = {}  # 储存与表名模式正则匹配的表
    not_pattern_dict = {}  # 储存与表名模式正则不匹配的表
    for table_name in source_tableList:
        r = re.match(pattern=source_pattern, string=table_name)
        if not r:
            not_pattern_dict[table_name] = {"source_table": table_name, "target_table": None}
        else:
            source_dict[r.group(1)] = {"source_table": table_name}
    # 找出目标表中符合规则的表
    for table_name in target_tableList:
        r = re.match(pattern=traget_pattern, string=table_name)
        if not r:
            not_pattern_dict[table_name] = {"source_table": None, "target_table": table_name}
        else:
            target_dict[r.group(1)] = {"target_table": table_name}

    # 找出差异
    all_keys = source_dict.keys() | target_dict.keys()
    common_keys = source_dict.keys() & target_dict.keys()
    diff_keys = all_keys - common_keys
    # 字典组合成新结果
    for i in common_keys:
        pattern_dict[i] = dict(**source_dict[i], **target_dict[i])
    for i in diff_keys:
        if i in source_dict:
            pattern_dict[i] = dict(**source_dict[i], target_table=None)
        elif i in target_dict:
            pattern_dict[i] = dict(**target_dict[i], source_table=None)
    result = dict(**pattern_dict, **not_pattern_dict)
    return result


def count_table(db_conn, sql):
    try:
        db_coursor = db_conn.cursor()
        db_coursor.execute(sql)
        rows = db_coursor.fetchall()
        return rows[0][0]
    except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
        # 该表不存在时，记录数返回None，并回滚事务
        db_conn.rollback()
    except Exception as e:
        traceback.print_exc()
        db_conn.rollback()


def compare_table_records(compare_data_dict, now_time, source_old_time, target_old_time, source_conn, target_conn):
    compare_result_flag = 1
    compare_result_list = []
    for pattern_group, table_realation in compare_data_dict.items():
        temp_dict = {"source_table": {}, "target_table": {}, "is_same": 0}
        for k, v in table_realation.items():  # 真正的需要对比数据
            if v == None:  # 表名为空,不需要进行查询
                temp_dict["is_same"] = 0
                compare_result_flag = 0
            else:
                # 查询数据库
                if k == "source_table":
                    # 执行的sql语句
                    if source_old_time:
                        source_sql = f"SELECT count(*) FROM {v} where update_time between '{source_old_time}' and '{now_time}'"
                        # source_sql = f"SELECT count(*) FROM {v} where sys_update_date between '{source_old_time}' and '{now_time}'"
                    else:
                        source_sql = f"SELECT count(*) FROM {v}"
                    temp_dict["source_table"]["table_name"] = v
                    temp_dict["source_table"]["count"] = count_table(source_conn, source_sql)
                elif k == "target_table":
                    if target_old_time:
                        target_sql = f"SELECT count(*) FROM {v} where update_time between '{target_old_time}' and '{now_time}'"
                        # target_sql = f"SELECT count(*) FROM {v} where sys_update_date between '{target_old_time}' and '{now_time}'"
                    else:
                        target_sql = f"SELECT count(*) FROM {v}"
                    temp_dict["target_table"]["table_name"] = v
                    temp_dict["target_table"]["count"] = count_table(target_conn, target_sql)
        # 对比结果标记
        if len(temp_dict['source_table'].keys()) and len(temp_dict['target_table'].keys()):
            # 两个字典都不为空时，对比数量
            source_table_count = temp_dict['source_table']['count']
            target_table_count = temp_dict['target_table']['count']
            if source_table_count == target_table_count and all([source_table_count, target_table_count]):
                temp_dict['is_same'] = 1
            else:
                temp_dict['is_same'] = 0
                compare_result_flag = 0
        compare_result_list.append(temp_dict)
    return compare_result_list, compare_result_flag


def compare_table_structure(compare_data_dict, source_conn, target_conn):
    compare_result = {"fields": {"list": [], "result": 1}, "constraints": {"list": [], "result": 1},
                      "indexs": {"list": [], "result": 1}, "result": 0}
    for pattern_group, table_realation in compare_data_dict.items():
        fields_temp_dict = {}  # 单个结果
        # 给临时字典字段赋值
        source_table_name = table_realation['source_table']
        target_table_name = table_realation['target_table']
        fields_temp_dict['source_table_name'] = source_table_name
        fields_temp_dict['target_table_name'] = target_table_name
        # 表字段对比方法
        source_fields = get_table_fields_info(db_conn=source_conn, table_name=source_table_name,
                                              db_type=1)
        target_fields = get_table_fields_info(db_conn=target_conn, table_name=target_table_name,
                                              db_type=2)
        fields_result, fields_flag = compare_table_fields_info(source_fields, target_fields)
        fields_temp_dict['fields_info'] = fields_result
        fields_temp_dict['fields_result'] = fields_flag
        compare_result["fields"]["list"].append(fields_temp_dict)
        if fields_flag == 0:
            compare_result["fields"]["result"] = 0
        # 对比表约束
        constraints_temp_dict = {}  # 单个结果
        constraints_temp_dict['source_table_name'] = source_table_name
        constraints_temp_dict['target_table_name'] = target_table_name
        source_constraints = get_table_constraints_info(db_conn=source_conn,
                                                        table_name=source_table_name, db_type=1)
        target_constraints = get_table_constraints_info(db_conn=target_conn,
                                                        table_name=target_table_name, db_type=2)
        constraints_result, constraints_flag = compare_table_constraint(source_constraints,
                                                                         target_constraints)
        constraints_temp_dict['constraints_info'] = constraints_result
        constraints_temp_dict['constraints_result'] = constraints_flag
        compare_result["constraints"]["list"].append(constraints_temp_dict)
        if constraints_flag == 0:
            compare_result["constraints"]["result"] = 0
        # 对比表索引
        indexs_temp_dict = {}  # 单个结果
        indexs_temp_dict['source_table_name'] = source_table_name
        indexs_temp_dict['target_table_name'] = target_table_name
        source_indexs = get_table_indexs_info(db_conn=source_conn,
                                              table_name=source_table_name, db_type=1)
        target_indexs = get_table_indexs_info(db_conn=target_conn,
                                              table_name=target_table_name, db_type=2)
        indexs_result, indexs_flag = compare_table_index(source_indexs, target_indexs)
        indexs_temp_dict['indexs_info'] = indexs_result
        indexs_temp_dict['indexs_result'] = indexs_flag
        compare_result["indexs"]["list"].append(indexs_temp_dict)
        if indexs_flag == 0:
            compare_result["indexs"]["result"] = 0
    if all([compare_result['fields']['result'], compare_result['constraints']['result'],
            compare_result['indexs']['result']]):
        compare_result['result'] = 1
    return compare_result


# class CountModel(BaseModel):
#     source_host: str = Field(...)
#     source_database: str = Field(...)
#     source_user: str = Field(...)
#     source_password: str = Field(...)
#     source_insert_start: str = Field(default=None)
#     source_insert_end: str = Field(default=None)
#     source_update_start: str = Field(default=None)
#     source_update_end: str = Field(default=None)
#     target_host: str = Field(...)
#     target_database: str = Field(...)
#     target_user: str = Field(...)
#     target_password: str = Field(...)
#     target_insert_start: str = Field(default=None)
#     target_insert_end: str = Field(default=None)
#     target_update_start: str = Field(default=None)
#     target_update_end: str = Field(default=None)
#     table_list: List[str] = Field(...)
#     view_list: List[str] = Field(...)
#     matview_list: List[str] = Field(...)
#
#     @validator('source_insert_start', 'source_insert_end',
#                'source_update_start', 'source_update_end',
#                'target_insert_start', 'target_insert_end',
#                'target_update_start', 'target_update_end',
#                pre=True)
#     def check_datetime(cls, str_date):
#         if isinstance(str_date, str) and str_date != None:
#             try:
#                 datetime.strptime(str_date, "%Y-%m-%d %H:%M:%S")
#             except Exception as e:
#                 raise ValueError(e)
#             return str_date
#         return str_date


# class ResultModel(IntEnum):
#     fail = 0  # 数量对比
#     success = 1  # 结构对比
#
#
# class CountResultSaveModel(CountModel):
#     """数据库表结构对比结果保存模型"""
#     result: ResultModel = Field(description="对比结果，0：失败 1：成功")
#     task_name: str = Field(...)
#     task_type: str = Field(...)
#     project_id: str = Field(...)
#     remarks: str = Field(default=None)
#     config_id: str = Field(default=None)
#     task_time: str = Field(default=None)
#
#     @validator('task_time', pre=True)
#     def check_datetime(cls, task_time):
#         if isinstance(task_time, str) and task_time != None:
#             try:
#                 CronTrigger.from_crontab(task_time)
#             except Exception as e:
#                 logging.error(str(e).strip())
#                 raise ValueError(e)
#             return task_time
#         return task_time


# class StructModel(BaseModel):
#     """数据库表结构对比模型"""
#     compare_type: CompareType = Field(default=1, description="对比方式，0：数量 1：结构")
#     source_host: str = Field(...)
#     source_database: str = Field(...)
#     source_user: str = Field(...)
#     source_password: str = Field(...)
#     target_host: str = Field(...)
#     target_database: str = Field(...)
#     target_user: str = Field(...)
#     target_password: str = Field(...)
#     table_list: List[str] = Field(...)
#     view_list: List[str] = Field(...)
#     matview_list: List[str] = Field(...)


# 现在不用了
# def get_table_fields_info(db_conn, table_list: List, db_type=1):
#     """
#     获取表的字段信息
#     @param table_list:数据库连接游标
#     @param table_list:表名列表
#     @param db_type:表类型，1为源表，2为目标表
#     @return:
#     """
#     column_type = None
#     is_null = None
#     if db_type == 1:
#         column_type = "source_column_type"
#         is_null = "source_is_null"
#     elif db_type == 2:
#         column_type = "target_column_type"
#         is_null = "target_is_null"
#     db_conn_cursor = db_conn.cursor()
#     result = {}
#     for table_name in table_list:
#         get_table_fields_info_sql = f"SELECT aa.schema_name,aa.table_name,aa.column_name,aa.column_type,aa.column_length,aa.is_null,f.description FROM(SELECT d.nspname AS SCHEMA_NAME,A.relname AS TABLE_NAME,b.attname AS COLUMN_NAME,C.typname AS column_type,b.attlen AS column_length,b.attnotnull AS is_null,b.attrelid,b.attnum FROM pg_class A,pg_attribute b,pg_type C,pg_namespace d WHERE A.relkind = 'r' AND A.relnamespace = d.oid AND A.oid = b.attrelid AND C.oid = b.atttypid AND b.attnum > 0 AND NOT b.attisdropped AND a.relname='{table_name}') aa LEFT JOIN pg_description f ON aa.attrelid = f.objoid AND aa.attnum = f.objsubid ORDER BY aa.table_name;"
#         try:
#             db_conn_cursor.execute(get_table_fields_info_sql)
#             rows = db_conn_cursor.fetchall()
#             # 只需要字段名、字段类型、是否为空三个
#             temp_dict = {}
#             for i in rows:
#                 temp_dict[i[2]] = {column_type: i[3], is_null: i[5]}
#             result[table_name] = {"fields": {}}
#             result[table_name]["fields"] = temp_dict
#         except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
#             logging.error(traceback.format_exc())
#             # 该表不存在时，记录数返回None，并回滚事务
#             db_conn.rollback()
#             return False
#         except Exception as e:
#             logging.error(traceback.format_exc())
#             db_conn.rollback()
#             return False
#     return result


# def compare_table_fields_info(source_filds_dict: dict, target_filds_dict: dict, table_list):
#     """
#     表字段对比
#     @param source_filds_dict: 源表字段信息
#     @param target_filds_dict: 目标表字段信息
#     @param table_list: 对比的表列表
#     @return: 对比
#     """
#     compare_flag = 1
#     same_flag = 1
#     result = []
#     for table in table_list:
#         single_table_result = {table: {"fields": {}}}
#         try:
#             all_keys = source_filds_dict[table]['fields'].keys() | target_filds_dict[table]['fields'].keys()
#             common_keys = source_filds_dict[table]['fields'].keys() & target_filds_dict[table]['fields'].keys()
#         except KeyError:
#             logging.error(traceback.format_exc())
#             single_table_result[table]["fields_result"] = None
#             result.append(single_table_result)
#             continue
#         if len(all_keys) == 0 and len(common_keys) == 0:
#             single_table_result[table]["fields_result"] = None
#             result.append(single_table_result)
#             continue
#         # 都有的key
#         for key in common_keys:
#             temp_dict = dict(**source_filds_dict[table]['fields'][key], **target_filds_dict[table]['fields'][key])
#             # 校验内容是否相同
#             if temp_dict["source_column_type"] == temp_dict['target_column_type'] and temp_dict['source_is_null'] == \
#                     temp_dict['target_is_null']:
#                 temp_dict.update({"is_same": 1})
#             else:
#                 temp_dict.update({"is_same": 0})
#                 same_flag = 0
#                 compare_flag = 0  # 主要有一个为0，整个对比结果就是0
#             single_table_result[table]["fields"][key] = temp_dict
#         # 其余key
#         unknow_keys = all_keys - common_keys
#         if len(unknow_keys) != 0:
#             same_flag = 0
#             compare_flag = 0
#             for key in unknow_keys:
#                 if key in source_filds_dict[table]['fields']:
#                     # 多的key在源表
#                     single_table_result[table]["fields"][key] = source_filds_dict[table]["fields"][key]
#                     single_table_result[table]["fields"][key].update(
#                         {"target_column_type": None, "target_is_null": None, "is_same": 0})
#                 elif key in target_filds_dict[table]["fields"]:
#                     # 多的key在目标表
#                     single_table_result[table]["fields"][key] = target_filds_dict[table]["fields"][key]
#                     single_table_result[table]["fields"][key].update(
#                         {"source_column_type": None, "sourceis_null": None, "is_same": 0})
#         if same_flag == 0:
#             compare_flag = 0
#             if len(single_table_result[table]["fields"]) == 0:  # 不为0的说明两个表中没有字段
#                 single_table_result[table]['fileds_result'] = None
#             else:
#                 single_table_result[table]['fileds_result'] = 0
#         else:
#             single_table_result[table]['fileds_result'] = 1
#         result.append(single_table_result)
#         same_flag = 1  # 重置标记
#     return result, compare_flag


# def get_table_constraints_info(db_conn, table_list: List, db_type=1):
#     """
#     获取表的约束信息
#     @param db_type:表类型，1为源表，2为目标表
#     @return:
#     """
#     constraint_type = None
#     if db_type == 1:
#         constraint_type = "source_constraint_type"
#     elif db_type == 2:
#         constraint_type = "target_constraint_type"
#     db_conn_cursor = db_conn.cursor()
#     result = {}
#     # 键值对常量
#     constant = {
#         "u": "unique",
#         "p": "primary key",
#         'f': "foreign key"
#     }
#     for table_name in table_list:
#         get_table_constraints_info_sql = f" select a.relname as tablename,'table constraint' ,c.conname as constraint_name,c.contype::varchar as constraint_type from pg_class a,pg_constraint c,pg_namespace d where a.relkind in ('r','p') and a.relnamespace=d.oid and a.oid=c.conrelid and a.relname='{table_name}' union all select a.relname as tablename,'column constraint',b.attname as column_name, 'not null' from pg_class a,pg_attribute b,pg_namespace c where a.relkind in ('r','p') and a.relnamespace=c.oid and b.attnum>0 and a.oid=b.attrelid and a.relname='{table_name}' and b.attnotnull;"
#         try:
#             db_conn_cursor.execute(get_table_constraints_info_sql)
#             rows = db_conn_cursor.fetchall()
#             # rows = [('约束1', 'PRIMARY KEY'), ('约束2', 'FOREIGN  KEY')]
#             temp_dict = {}
#             for i in rows:
#                 temp_dict[i[2]] = {constraint_type: constant[i[3]] if i[3] in constant else i[3]}  # 当类型是u/p/f时进行转换
#             result[table_name] = {"constraints": {}}
#             result[table_name]["constraints"] = temp_dict
#         except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
#             logging.error(traceback.format_exc())
#             # 该表不存在时，记录数返回None，并回滚事务
#             db_conn.rollback()
#             return False
#         except Exception as e:
#             logging.error(traceback.format_exc())
#             db_conn.rollback()
#             return False
#     return result


# def compare_table_constraint(source_constraint_dict: dict, target_constraint_dict: dict, table_list):
#     same_flag = 1
#     compare_flag = 1
#     result = []
#     for table in table_list:
#         single_table_result = {table: {"constraints": {}}}
#         try:
#             all_keys = source_constraint_dict[table]['constraints'].keys() | target_constraint_dict[table][
#                 'constraints'].keys()
#             common_keys = source_constraint_dict[table]['constraints'].keys() & target_constraint_dict[table][
#                 'constraints'].keys()
#         except KeyError:
#             logging.error(traceback.format_exc())
#             single_table_result[table]["constraints_result"] = None
#             result.append(single_table_result)
#             continue
#         if len(all_keys) == 0 and len(common_keys) == 0:
#             single_table_result[table]["constraints_result"] = None
#             result.append(single_table_result)
#             continue
#         # 都有的key
#         for key in common_keys:
#             temp_dict = dict(**source_constraint_dict[table]['constraints'][key],
#                              **target_constraint_dict[table]['constraints'][key])
#             # 校验内容是否相同
#             if temp_dict["source_constraint_type"] == temp_dict['target_constraint_type']:
#                 temp_dict.update({"is_same": 1})
#             else:
#                 temp_dict.update({"is_same": 0})
#                 same_flag = 0
#                 compare_flag = 0  # 只要有一个为0，整个对比结果就是0
#             single_table_result[table]["constraints"][key] = temp_dict
#         # 其余key
#         unknow_keys = all_keys - common_keys
#         if len(unknow_keys) != 0:
#             same_flag = 0
#             compare_flag = 0
#             for key in unknow_keys:
#                 if key in source_constraint_dict[table]['constraints']:
#                     # 多的key在源表
#                     single_table_result[table]["constraints"][key] = source_constraint_dict[table]["constraints"][key]
#                     single_table_result[table]["constraints"][key].update(
#                         {"target_constraint_type": None, "is_same": 0})
#                 elif key in target_constraint_dict[table]["constraints"]:
#                     # 多的key在目标表
#                     single_table_result[table]["constraints"][key] = target_constraint_dict[table]["constraints"][key]
#                     single_table_result[table]["constraints"][key].update(
#                         {"source_constraint_type": None, "is_same": 0})
#         if same_flag == 0:
#             compare_flag = 0
#             if len(single_table_result[table]["constraints"]) == 0:  # 不为0的说明两个表中没有字段
#                 single_table_result[table]['constraints_result'] = None
#             else:
#                 single_table_result[table]['constraints_result'] = 0
#         else:
#             single_table_result[table]['constraints_result'] = 1
#         result.append(single_table_result)
#         same_flag = 1  # 重置标记
#     return result, compare_flag


# def get_table_indexs_info(db_conn, table_list: List, db_type=1):
#     """
#     获取表的索引信息
#     @param db_conn: 数据库连接对象
#     @param table_list:表名列表
#     @param db_type:表类型，1为源表，2为目标表
#     @return:
#     """
#     index_name, index_columns, index_null, index_unique = None, None, None, None
#     if db_type == 1:
#         index_columns = "source_index_columns"
#         index_null = "source_index_null"
#         index_unique = "source_index_unique"
#     elif db_type == 2:
#         index_columns = "target_index_columns"
#         index_null = "target_index_null"
#         index_unique = "target_index_unique"
#     db_conn_cursor = db_conn.cursor()
#     result = {}
#     for table_name in table_list:
#         get_table_index_info_sql = f"select n.nspname as schemaname,a.relname as tablename,i.relname as indexname,m.amname as indextype,array(select pg_get_indexdef(idx.indexrelid,k+1,true) from generate_subscripts(idx.indkey,1) as k order by k) as index_columns,idx.indisunique as is_unique,idx.indisprimary as is_pk from pg_class a,pg_class i,pg_index idx,pg_namespace n,pg_am m where a.relnamespace=n.oid and a.relkind in ('r','p') and i.relkind='i' and a.oid=idx.indrelid and i.oid=idx.indexrelid and i.relam=m.oid and a.relname='{table_name}';"
#         try:
#             db_conn_cursor.execute(get_table_index_info_sql)
#             rows = db_conn_cursor.fetchall()
#             temp_dict = {}
#             for i in rows:
#                 # i[2]是索引名
#                 temp_dict[i[2]] = {index_columns: ','.join(i[4]), index_null: i[6], index_unique: i[5]}
#             result[table_name] = {"indexs": {}}
#             result[table_name]["indexs"] = temp_dict
#         except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
#             print(e)
#             logging.error(traceback.format_exc())
#             # 该表不存在时，记录数返回None，并回滚事务
#             db_conn.rollback()
#             return False
#         except Exception as e:
#             logging.error(traceback.format_exc())
#             db_conn.rollback()
#             return False
#     return result


# def compare_table_index(source_index_dict: dict, target_index_dict: dict, table_list):
#     same_flag = 1
#     compare_flag = 1
#     result = []
#     for table in table_list:
#         single_table_result = {table: {"indexs": {}}}
#         try:
#             all_keys = source_index_dict[table]['indexs'].keys() | target_index_dict[table]['indexs'].keys()
#             common_keys = source_index_dict[table]['indexs'].keys() & target_index_dict[table]['indexs'].keys()
#         except KeyError as e:
#             logging.error(traceback.format_exc())
#             single_table_result[table]["indexs_result"] = None
#             result.append(single_table_result)
#             continue
#         if len(all_keys) == 0 and len(common_keys) == 0:
#             single_table_result[table]["indexs_result"] = None
#             result.append(single_table_result)
#             continue
#         # 都有的key
#         for key in common_keys:
#             temp_dict = dict(**source_index_dict[table]['indexs'][key], **target_index_dict[table]['indexs'][key])
#             # 校验内容是否相同
#             if temp_dict["source_index_columns"] == temp_dict['target_index_columns'] and temp_dict[
#                 "source_index_null"] == temp_dict[
#                 'target_index_null'] and temp_dict["source_index_unique"] == temp_dict['target_index_unique']:
#                 temp_dict.update({"is_same": 1})
#             else:
#                 temp_dict.update({"is_same": 0})
#                 same_flag = 0
#                 compare_flag = 0  # 只要有一个为0，整个对比结果就是0
#             single_table_result[table]['indexs'][key] = temp_dict
#         # 其余key
#         unknow_keys = all_keys - common_keys
#         if len(unknow_keys) != 0:
#             same_flag = 0
#             compare_flag = 0
#             for key in unknow_keys:
#                 if key in source_index_dict[table]['indexs']:
#                     # 多的key在源表
#                     single_table_result[table]['indexs'][key] = source_index_dict[table]['indexs'][key]
#                     single_table_result[table]['indexs'][key].update(
#                         {"target_index_columns": None, "target_index_null": None, "target_index_unique": None,
#                          "is_same": 0})
#                 elif key in target_index_dict[table]['indexs']:
#                     # 多的key在目标表
#                     single_table_result[table]['indexs'][key] = target_index_dict[table]['indexs'][key]
#                     single_table_result[table]['indexs'][key].update(
#                         {"source_index_columns": None, "source_index_null": None, "source_index_unique": None,
#                          "is_same": 0})
#         if same_flag == 0:
#             compare_flag = 0
#             if len(single_table_result[table]["indexs"]) == 0:  # 不为0的说明两个表中没有字段
#                 single_table_result[table]['indexs_result'] = None
#             else:
#                 single_table_result[table]['indexs_result'] = 0
#         else:
#             single_table_result[table]['indexs_result'] = 1
#         result.append(single_table_result)
#         same_flag = 1  # 重置标记
#     return result, compare_flag


# def get_views_info(db_conn, views_list: List, db_type=1):
#     """
#     获取表的视图信息
#     @param db_conn:数据库连接游标
#     @param table_list:表名列表
#     @param db_type:表类型，1为源表，2为目标表
#     @return:
#     """
#     definition_type = None
#     if db_type == 1:
#         definition_type = "source_definition"
#     elif db_type == 2:
#         definition_type = "target_definition"
#     db_conn_cursor = db_conn.cursor()
#     result = {}
#     for view_name in views_list:
#         get_table_fields_info_sql = f"select definition from pg_views where viewname='{view_name}';"
#         try:
#             db_conn_cursor.execute(get_table_fields_info_sql)
#             rows = db_conn_cursor.fetchall()
#             # 只需要字段名、字段类型、是否为空三个
#             temp_dict = {}
#             for i in rows:
#                 temp_dict[definition_type] = i[0]
#             result[view_name] = {"definition": {}}
#             result[view_name]["definition"] = temp_dict
#         except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
#             logging.error(traceback.format_exc())
#             # 该表不存在时，记录数返回None，并回滚事务
#             db_conn.rollback()
#             return False
#         except Exception as e:
#             logging.error(traceback.format_exc())
#             db_conn.rollback()
#             return False
#     return result


# def compare_view(source_view_dict: dict, target_view_dict: dict, view_list):
#     result = []
#     compare_flag = 1
#     # 自测用数据
#     # view_list=['view1','view2']
#     # source_view_dict={
#     #     'view1':{"definition":{"source_definition":'12134'}},
#     #     'view2': {"definition": {}},
#     # }
#     # target_view_dict = {
#     #     'view1': {"definition": {"target_definition":'12134'}},
#     #     'view2': {"definition": {"target_definition":'12134'}},
#     # }
#     for view in view_list:
#         single_table_result = {view: {"definition": {}}}
#         if source_view_dict[view]['definition'] == {} and target_view_dict[view]['definition'] == {}:
#             single_table_result[view]["definition"] = {"is_same": None}
#             single_table_result[view]['definition_result'] = None
#             result.append(single_table_result)
#             continue
#         try:
#             if source_view_dict[view]['definition']['source_definition'] == target_view_dict[view]['definition'][
#                 'target_definition']:
#                 single_table_result[view]["definition"] = {"is_same": 1}
#                 single_table_result[view]['definition_result'] = 1
#             else:
#                 single_table_result[view]["definition"] = {"is_same": 0}
#                 single_table_result[view]['definition_result'] = 0
#                 compare_flag = 0  # 只要有一个为0，整个对比结果就是0
#         except KeyError:
#             logging.error(traceback.format_exc())
#             single_table_result[view]["definition"] = {"is_same": 0}
#             single_table_result[view]['definition_result'] = 0
#             compare_flag = 0
#         result.append(single_table_result)
#     return result, compare_flag


# def get_matview_info(db_conn, matview_list: List, db_type=1):
#     """
#     获取表的视图信息
#     @param db_conn:数据库连接游标
#     @param matview_list:物化视图列表
#     @param db_type:表类型，1为源表，2为目标表
#     @return:
#     """
#     definition_type = None
#     if db_type == 1:
#         definition_type = "source_definition"
#     elif db_type == 2:
#         definition_type = "target_definition"
#     db_conn_cursor = db_conn.cursor()
#     result = {}
#     for view_name in matview_list:
#         get_table_fields_info_sql = f"select definition from pg_matviews where matviewname='{view_name}';"
#         try:
#             db_conn_cursor.execute(get_table_fields_info_sql)
#             rows = db_conn_cursor.fetchall()
#             # 只需要字段名、字段类型、是否为空三个
#             temp_dict = {}
#             for i in rows:
#                 temp_dict[definition_type] = i[0]
#             result[view_name] = {"definition": {}}
#             result[view_name]["definition"] = temp_dict
#         except psycopg2.errors.lookup(UNDEFINED_TABLE) as e:
#             logging.error(traceback.format_exc())
#             # 该表不存在时，记录数返回None，并回滚事务
#             db_conn.rollback()
#             return False
#         except Exception as e:
#             logging.error(traceback.format_exc())
#             db_conn.rollback()
#             return False
#     return result

#
# def compare_matview(source_matview_dict: dict, target_matview_dict: dict, view_list):
#     result = []
#     compare_flag = 1
#     # 自测用数据
#     # view_list=['view1','view2']
#     # source_view_dict={
#     #     'view1':{"definition":{"source_definition":'12134'}},
#     #     'view2': {"definition": {}},
#     # }
#     # target_view_dict = {
#     #     'view1': {"definition": {"target_definition":'12134'}},
#     #     'view2': {"definition": {"target_definition":'12134'}},
#     # }
#     for view in view_list:
#         single_table_result = {view: {"definition": {}}}
#         if source_matview_dict[view]['definition'] == {} and target_matview_dict[view]['definition'] == {}:
#             single_table_result[view]["definition"] = {"is_same": None}
#             single_table_result[view]['definition_result'] = None
#             result.append(single_table_result)
#             continue
#         try:
#             if source_matview_dict[view]['definition']['source_definition'] == target_matview_dict[view]['definition'][
#                 'target_definition']:
#                 single_table_result[view]["definition"] = {"is_same": 1}
#                 single_table_result[view]['definition_result'] = 1
#             else:
#                 single_table_result[view]["definition"] = {"is_same": 0}
#                 single_table_result[view]['definition_result'] = 0
#                 compare_flag = 0  # 只要有一个为0，整个对比结果就是0
#         except KeyError:
#             logging.error(traceback.format_exc())
#             single_table_result[view]["definition"] = {"is_same": 0}
#             single_table_result[view]['definition_result'] = 0
#             compare_flag = 0
#         result.append(single_table_result)
#     return result, compare_flag


# 新的新的


if __name__ == '__main__':
    # 表字段属性对比示例
    # source = {
    #     "table1":
    #         {
    #             "fields": {
    #                 "fild2": {'source_column_type': 'text', 'source_is_null': True},
    #                 "fild3": {'source_column_type': 'int', 'source_is_null': True},
    #                 # "fild4": {'source_column_type': 'int2', 'source_is_null': True}
    #             }
    #         },
    #     "table2": {
    #     }
    # }
    # target = {
    #     "table1":
    #         {
    #             "fields": {
    #                 "fild1": {'target_column_type': 'varchar', 'target_is_null': True},
    #                 "fild3": {'target_column_type': 'int', 'target_is_null': True},
    #                 # "fild4": {'target_column_type': 'int2', 'target_is_null': True}
    #             }
    #         },
    #     "table2": {
    #     }
    # }
    # print(compare_table_fields_info(source, target, ["table1", "table2"]))

    # print(compare_table_fields_info(source, target, ["table1", "table2"])[0])

    # # 约束对比示例
    # source = {'ads_brain_admini_penalty': {
    #     'constraints': {
    #         'ads_brain_admini_penalty_unique_ds_id': {'source_constraint_type': 'unique'},
    #         'ods_brain_admini_penalty_pkey': {'source_constraint_type': 'primary key'},
    #         'id': {'source_constraint_type': 'not null'},
    #     }},
    #     'ads_brain_area_industry_evaluation11222211': {'constraints': {'id': {'source_constraint_type': 'not null'}}}}
    #
    # target = {'ads_brain_admini_penalty': {
    #     'constraints': {
    #         'ads_brain_admini_penalty_unique_ds_id': {'target_constraint_type': 'unique'},
    #         'ods_brain_admini_penalty_pkey': {'target_constraint_type': 'primary key'},
    #         'id': {'target_constraint_type': 'not null'},
    #     }},
    #     'ads_brain_area_industry_evaluation11222211': {'constraints': {'id': {'target_constraint_type': 'not null'}}}}
    # # print(compare_table_constraint(source, target, ['ads_brain_admini_penalty','ads_brain_area_industry_evaluation11222211']))
    # # # 索引对比示例
    # source = {'ads_brain_admini_penalty': {
    #     "indexs": {
    #         'index_name1': {'source_index_columns': 'filed1,filed3', 'source_index_null': True,
    #                         'source_index_unique': True},
    #         'index_name2': {'source_index_columns': 'filed3', 'source_index_null': False, 'source_index_unique': False},
    #         'index_name3': {'source_index_columns': 'filed4', 'source_index_null': False, 'source_index_unique': False},
    #     }
    # }, "table2": {"indexs": {'index_name1': {'source_index_columns': 'filed1,filed3', 'source_index_null': True,
    #                                          'source_index_unique': True}}}
    # }
    #
    # target = {'ads_brain_admini_penalty': {
    #     "indexs": {
    #         'index_name1': {'target_index_columns': 'filed1,filed2', 'target_index_null': True,
    #                         'target_index_unique': True},
    #         'index_name2': {'target_index_columns': 'filed3', 'target_index_null': False, 'target_index_unique': False},
    #         'index_name4': {'target_index_columns': 'filed5', 'target_index_null': False, 'target_index_unique': False},
    #     }
    # }, "table2": {"indexs": {'index_name1': {'target_index_columns': 'filed1,filed3', 'target_index_null': True,
    #                                          'target_index_unique': True}}}}
    # print(compare_table_index(source, target, ['ads_brain_admini_penalty', 'table2']))
    # print(compare_table_index(source, target, ['table2', 'ads_brain_admini_penalty1']))
    data = {"reuslt": 1,
            "list": [
                {"source_table": "", "target_table": "", "is_same": 1}
            ]}
    source_model = "ds_*_#"
    target_model = "ods_pub_*"
    source_table = [
        "ds_bidding_5405",
        "ds_cqc_voluntary_product_certification_4313",
        "ds_bsds_3433",
        "ds_sdsgerg_dsd",
        'sds'
    ]
    target_table = [
        "ods_pub_cqc_voluntary_product_certification",
        "ods_pub_bidding",
        "ds_djf119_2011_qwd",
        "sad_sdfsdfs",
        "ods_pub_sffffkkkkk"
    ]
    get_compare_data(source_pattern=source_model, source_tableList=source_table, target_pattern=target_model,
                     target_tableList=target_table)
