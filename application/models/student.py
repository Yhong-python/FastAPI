# coding=utf-8
"""
File: student.py
Created on 2022/3/8 17:26
__author__= yangh
__remark__=
"""
from application import db

#一对多
class Student(db.Model):
    """学生信息"""
    __tablename__ = "student"
    id = db.Column(db.Integer, primary_key=True, comment="主键ID")
    name = db.Column(db.String(64), index=True, comment="姓名")
    sex = db.Column(db.Enum('0','1'), server_default='0', comment="性别",nullable=False)
    class_number = db.Column(db.String(32), nullable=True, index=True, comment="班级")
    age = db.Column(db.SmallInteger, comment="年龄")
    description = db.Column(db.Text, comment="个性签名")
    name2=db.relationship('Course',backref='student',lazy="dynamic")
    description1111111 = db.Column(db.Text, comment="个性1111签名")



def get_all_studends_info():
    r = Student.query.first()
    return r
