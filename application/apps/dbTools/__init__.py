# coding=utf-8
"""
File: __init__.py.py
Created on 2022/3/28 9:18
__author__= yangh
__remark__=
"""
from flask import Blueprint


dbtools_blu = Blueprint('dbTools', __name__)
from .dbConfig_view import *
from .task_view import *
from .compare_view import *
