import time
import json
import pymysql.cursors
from pysqler import Insert, Update
import setting
from mysql import get_conn

focus_database = 'demo'
conn = get_conn(focus_database)


for index in range(0, 10000000):
    try:
        command = Update("uorder")

        command.put("amount", "amount + 1")

        with conn.cursor() as cursor:
            sql = str(command)
            cursor.execute(sql)
            conn.commit()
        time.sleep(0.1)
        print(index)

    except Exception as e:
        print(e)
        time.sleep(60)
        conn.close()
        conn = get_conn()
