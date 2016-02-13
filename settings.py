import os, MySQLdb as mdb


def connect_to_mysql(config):

    mysql_host = config['mysql']['host']
    mysql_user = config['mysql']['user']
    mysql_password = config['mysql']['password']
    mysql_dbname = config['mysql']['database']

    return mdb.connect(mysql_host, mysql_user, mysql_password, mysql_dbname, charset='utf8')


