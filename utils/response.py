# coding=utf-8
"""
File: response.py
Created on 2022/3/10 16:38
__author__= yangh
__remark__=
"""
import datetime
import decimal
import uuid
from flask.json import JSONEncoder as BaseJSONEncoder


class ResponseCode(object):
    SUCCESS = 1  # 成功
    FAIL = -1  # 失败
    NO_RESOURCE_FOUND = 40001  # 未找到资源
    INVALID_PARAMETER = 40002  # 参数无效


class ResponseMessage(object):
    SUCCESS = "成功"
    FAIL = "失败"
    NO_RESOURCE_FOUND = "未找到资源"
    INVALID_PARAMETER = "参数无效"


class ResMsg(object):
    """
    封装响应文本
    """

    def __init__(self, data=None, code=ResponseCode.SUCCESS,
                 msg=ResponseMessage.SUCCESS):
        self._data = data
        self._msg = msg
        self._code = code

    def update(self, code=None, data=None, msg=None):
        """
        更新默认响应文本
        :param code:响应状态码
        :param data: 响应数据
        :param msg: 响应消息
        :return:
        """
        if code is not None:
            self._code = code
        if data is not None:
            self._data = data
        if msg is not None:
            self._msg = msg

    def add_field(self, name=None, value=None):
        """
        在响应文本中加入新的字段，方便使用
        :param name: 变量名
        :param value: 变量值
        :return:
        """
        if name is not None and value is not None:
            self.__dict__[name] = value

    @property
    def data(self):
        """
        输出响应文本内容
        :return:
        """
        body = self.__dict__
        body["data"] = body.pop("_data")
        body["msg"] = body.pop("_msg")
        body["code"] = body.pop("_code")
        return body


class JSONEncoder(BaseJSONEncoder):
    """
    重新default方法，支持更多的转换方法
    """

    def default(self, o):
        """
        如有其他的需求可直接在下面添加
        :param o:
        :return:
        """
        if isinstance(o, datetime.datetime):
            # 格式化时间,将Thu, 10 Mar 2022 16:00:00 GMT形式的日期转出成2022-03-11 16:00:00
            return o.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(o, datetime.date):
            # 格式化日期,将Thu, 10 Mar 2022 16:00:00 GMT形式的日期转出成2022-03-11
            return o.strftime('%Y-%m-%d')
        if isinstance(o, decimal.Decimal):
            # 格式化高精度数字,不转化会报错
            return str(o)
        if isinstance(o, uuid.UUID):
            # 格式化uuid
            return str(o)
        if isinstance(o, bytes):
            # 格式化字节数据
            return o.decode("utf-8")
        return super(JSONEncoder, self).default(o)
