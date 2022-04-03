# coding=utf-8
from datetime import datetime
from application import db


class Configuration(db.Model):
    """源目的库配置"""
    __tablename__ = "configuration"
    id = db.Column(db.Integer, primary_key=True, comment="主键ID")
    name = db.Column(db.String(64), unique=True, comment="配置名称")
    type = db.Column(db.Integer, comment="数据配置类型 0：连接数据库对比 1：上传结果文件")
    host = db.Column(db.String(64), comment="主机ip")
    port = db.Column(db.Integer, comment="端口号")
    database = db.Column(db.String(64), comment="连接数据库名称")
    schema = db.Column(db.String(64))
    user = db.Column(db.String(64), comment="用户名")
    password = db.Column(db.String(64), comment="密码")
    create_time = db.Column(db.DateTime, default=datetime.now, comment="创建时间")
    update_time = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")


def add_config(name, host, database, schema, user, password, config_type):
    """
    添加源目的库配置
    @return:
    """
    config = Configuration(name=name, host=host, port=5432,
                           database=database, schema=schema,
                           user=user, password=password, type=config_type)
    try:
        db.session.add(config)
        db.session.commit()
    except Exception as e:
        print(e)
        db.session.rollback()
        return False
    return True


def config_name_is_exist(name):
    """
    判断配置名称是否重复
    @param name: 配置名称
    @return:
    """
    result = Configuration.query.filter_by(name=name).first()
    if result:
        return True
    return False


def update_config(config_id, name, host, database, schema, user, password, type):
    """
    修改配置信息
    """
    try:
        config = Configuration.query.filter_by(id=config_id).first()
        if not config:
            return False
        config.name = name
        config.host = host
        config.database = database
        config.schema = schema
        config.user = user
        config.password = password
        config.type = type
        db.session.commit()
    except Exception as e:
        print(e)
        db.session.rollback()
        return False
    return True


def get_config_by_id(config_id):
    """
    判断配置id是否存在
    @param config_id: 主键id
    @return:
    """
    result = Configuration.query.filter(Configuration.id == config_id).first()
    return result


def delete_config_by_id(config_id):
    """
    删除配置
    @param config_id: 配置id主键
    @return:
    """
    config_info = get_config_by_id(config_id)
    if not config_info:
        return False
    try:
        db.session.delete(config_info)
        db.session.commit()
    except Exception as e:
        print(e)
        db.session.rollback()
        return False
    return True


def configuration_query_all():
    """
    返回配置列表
    @return:
    """
    res = Configuration.query.order_by(Configuration.id).all()
    return res


def is_config_name_repeat(config_id, name):
    result = Configuration.query.filter(Configuration.id != config_id, Configuration.name == name).all()
    if result:
        return True
    return False
