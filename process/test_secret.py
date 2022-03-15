from secret import get_secret

mysql_secret_name = 'dev/demo/mysql'
mysql_info = get_secret(mysql_secret_name)
print(mysql_info)
