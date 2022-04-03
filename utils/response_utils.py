# coding=utf-8
"""
File: response_utils.py
Created on 2022/3/8 17:58
__author__= yangh
__remark__=
"""


class ResponseUtils:

    @staticmethod
    def pop_sa_instance_state(dict_data: dict):
        new_dict = {}
        for k, v in dict_data.items():
            if k == '_sa_instance_state':
                continue
            else:
                new_dict[k] = v
        return new_dict

    @staticmethod
    def get_dict_sql_result(data: list):
        if isinstance(data, list):
            result_list = []
            for i in data:
                result_list.append(ResponseUtils.pop_sa_instance_state(i.__dict__))
            return result_list
        else:
            return ResponseUtils.pop_sa_instance_state(data.__dict__)
