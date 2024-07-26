import config as cp
import mysql.connector as mysql
from mysql.connector import connection
import logging
import time
obj = cp.config()
cpProp=obj.config

def mysqlconnection():
    retrycount=0
    sqlconn=None
    while retrycount < 3:
        try:
            logging.info("Trying for mysql connection")
            sqlconn = mysql.connect(
                user=cpProp.dbCp['username'],
                password=cpProp.dbCp['password'],
                database=cpProp.dbCp['db'],
                host=cpProp.dbCp['host']
            )
            sqlconn.autocommit = True
            break
        except Exception as e:
            logging.error(f'Could not connect to database :: {e} :: retry count :: {retrycount}')
            retrycount += 1
            time.sleep(2)
    return sqlconn
