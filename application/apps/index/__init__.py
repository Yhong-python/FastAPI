# coding=utf-8
"""
File: __init__.py.py
Created on 2022/3/7 17:38
__author__= yangh
__remark__=
"""
from flask import Blueprint
users_blu=Blueprint('users',__name__)
from .views import *
