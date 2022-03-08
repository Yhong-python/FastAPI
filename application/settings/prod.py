# coding=utf-8
"""
File: prod.py
Created on 2022/3/7 17:15
__author__= yangh
__remark__=
"""

from . import Config
class ProductionConfig(Config):
    """生产模式下的配置"""
    DEBUG = False
