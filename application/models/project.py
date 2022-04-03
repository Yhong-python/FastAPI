# coding=utf-8
"""
File: project.py
Created on 2022/3/17 16:14
__author__= yangh
__remark__=
"""
from application import db

class Project(db.Model):
    """手工维护的项目表"""
    __tablename__ = "projects"
    id = db.Column(db.Integer, autoincrement=True,primary_key=True, comment="项目id")
    name = db.Column(db.String(64),comment="项目名称")
    tasks=db.relationship('Task',backref='projects',lazy="dynamic") #给Task表做外键

    # @property
    # def serialize(self):
    #     return {
    #         "id":self.id,
    #         "name":self.name
    #     }
