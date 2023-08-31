import time
import json
import pymysql.cursors
from pysqler import Insert
import setting
from mysql import get_conn
from mock import gen, DataType

focus_database = setting.DB
conn = get_conn(focus_database)

order_schema = {
    "user_mail": (DataType.Enum, ('barry.xu@163.com', 'dandan@qq.com', 'pony@qq.com', 'focus@qq.com')),
    "status": (DataType.Enum, ('unpaid', 'paid', 'cancel', 'shipping', 'finished')),
    "good_count": (DataType.INT, (1, 10)),
    "city":  (DataType.CITY,),
    "amount": (DataType.DOUBLE, (10, 1000)),
    "create_time": (DataType.DATETIME,),
    "update_time": (DataType.DATETIME,)
}

creator = gen(columns=order_schema,
              interval_min=100, interval_max=1000)

for item in creator:
    print(item)
    try:
        command = Insert("`{0}`".format("order"))
        for key in item:
            command.put(key, item[key])

        print("mock: {0}".format(item))
        with conn.cursor() as cursor:
            sql = str(command)
            cursor.execute(sql)
            conn.commit()

    except Exception as e:
        print(e)
        # time.sleep(60)
        # conn.close()
        # conn = get_conn()
