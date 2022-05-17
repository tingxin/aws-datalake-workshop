import time
import json
import pymysql.cursors
from pysqler import Insert
import setting
from mysql import get_conn
from mock import gen, DataType

focus_database = 'demo'
conn = get_conn(focus_database)

order_schema = {
    "name": (DataType.Enum, ('unpaid', 'paid', 'cancel', 'shipping', 'finished')),
    "description": (DataType.STR, (1, 10)),
    "weight": (DataType.DOUBLE, (10, 1000))
}

creator = gen(columns=order_schema,
              interval_min=100, interval_max=1000, increment_id='id')

for item in creator:
    print(item)
    try:
        command = Insert("`{0}`".format("simple2"))
        for key in item:
            command.put(key, item[key])

        print("mock: {0}".format(item))
        with conn.cursor() as cursor:
            sql = str(command)
            cursor.execute(sql)
            conn.commit()

    except Exception as e:
        print(e)
        time.sleep(60)
        conn.close()
        conn = get_conn()
