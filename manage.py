# coding=utf-8
"""
File: 111.py
Created on 2022/3/7 17:03
__author__= yangh
__remark__=
"""
from application import init_app, db
from flask_script import Manager,Server
from flask_migrate import Migrate, MigrateCommand
# 导入模型[为了进行数据迁移]
from application.apps.index.models import Student

app = init_app('dev')
# 使用终端脚本工具启动和管理flask
manager = Manager(app)
# 启用数据迁移工具
Migrate(app, db)
# 添加数据迁移的命令到终端脚本工具中
manager.add_command('db', MigrateCommand)
manager.add_command('start', Server(port=8000, use_debugger=True)) # 创建启动命令


@app.route('/')
def index():
    return 'index'


if __name__ == '__main__':
    # app.run()
    manager.run()

    #初始化或执行数据库更新命令
    # python manage.py db init   #初始化数据迁移的目录
    # python manage.py db migrate   #数据库的数据迁移版本初始化
    # python manage.py db upgrade  #升级版本
    # python manage.py db downgrade  #降级版本
