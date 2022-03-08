# coding=utf-8
"""
File: views.py
Created on 2022/3/7 17:38
__author__= yangh
__remark__=
"""
from . import users_blu
from flask import request
from .models import db,Student,get_all_studends_info


#显示学生信息
@users_blu.route('/students')
def students():
    return '1111111111'


@users_blu.route("/add",methods=["POST","GET"])
def add_student():
    if request.method == "POST":
        # 接受數據
        name = request.form.get("username")
        age = int( request.form.get("age") )if request.form.get("age") else 0
        sex = True if request.form.get("sex") == '1' else False
        class_number = request.form.get("class_number")
        description = request.form.get("description")

        # 保存入庫
        student = Student(name=name,age=age,sex=sex,class_number=class_number,description=description)
        try:
            db.session.add(student)
            db.session.commit()
        except Exception as e:
            # 事務回滾
            print(e)
            db.session.rollback()
    return 'success'

@users_blu.route("/get")
def query_student():
    r=get_all_studends_info()
    # print(r[0].__dict__)
    # for i in r:
    #     print(i.__dict__)
    #     print(dir(i))
    # print(dir(r))
    return '11111'
