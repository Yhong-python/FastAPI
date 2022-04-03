# coding=utf-8
"""
File: course.py
Created on 2022/3/8 17:26
__author__= yangh
__remark__=
"""
from application import db

class Course(db.Model):
    """课程信息"""
    __tablename__ = "course"
    id = db.Column(db.Integer, primary_key=True, comment="主键ID")
    course_name = db.Column(db.String(64), unique=True, comment="课程名称")
    name = db.Column(db.String(64),db.ForeignKey('student.name'),unique=True, comment="学生名称")
    grade = db.Column(db.Integer,comment="成绩")
