import oracledb

# 1. Включаем Thick-режим и указываем, где tnsnames.ora
oracledb.init_oracle_client(
    lib_dir=r'D:\instantclient_12_1',
    config_dir=r'D:\instantclient_12_1',
)

# 2. Подключаемся по TNS-алиасу из tnsnames.ora
conn = oracledb.connect(
    user='POOL3',
    password='dthf34',
    dsn='general',  # то, что после @ в sqlplus
    # encoding='UTF-8',
)

# 3. Пробный запрос
with conn.cursor() as cur:
    cur.execute("select 'OK' as status from dual")
    print(cur.fetchall())
