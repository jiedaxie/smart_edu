import pymysql
from pymysql.cursors import DictCursor

# 连接数据库
connection = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="123456"
)
cursor = connection.cursor(cursor=DictCursor)

# 要查询的数据库名
db_name = 'ai_edu'

# 第一步：查询 ai_edu 数据库下所有的表名
cursor.execute(f"""
    SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = '{db_name}'
""")
tables = cursor.fetchall()

# 第二步：遍历每个表，查询其 DDL 语句
for table in tables:
    table_name = table['TABLE_NAME']
    cursor.execute(f"SHOW CREATE TABLE {db_name}.{table_name}")
    create_table_result = cursor.fetchone()
    # `SHOW CREATE TABLE` 返回的结果中，键为 'Create Table' 的值就是该表的 DDL 语句
    ddl = create_table_result['Create Table']
    print(ddl)
    print("-" * 50)  # 分割线，方便查看不同表的 DDL

# 关闭游标和连接
cursor.close()
connection.close()

