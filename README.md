# Flask
1. 初始化数据迁移的目录
python manage.py db init
​
2. 数据库的数据迁移版本初始化
python manage.py db migrate -m 'initial migration'
​
3. 升级版本[创建表]
python manage.py db upgrade 
​
4. 降级版本[删除表]
python manage.py db downgrade
​
5. 运行程序
python manage.py start
