import psycopg2
from psycopg2 import OperationalError

def connect_to_opengauss():
    """连接 openGauss 数据库"""
    conn = None
    try:
        # 数据库连接参数（根据你的实际配置修改）
        conn = psycopg2.connect(
            host="localhost",
            port="8888",
            database="postgres",
            user="yyc",
            password="Yyc@123456+"
        )
        print("✅ 数据库连接成功！")
        return conn
    except OperationalError as e:
        print(f"❌ 连接失败：{e}")
        return None

def main():
    # 1. 连接数据库
    conn = connect_to_opengauss()
    if not conn:
        return  # 连接失败则退出

    try:
        # 2. 创建游标（用于执行 SQL）
        cursor = conn.cursor()

        # 3. 创建测试表（若不存在）
        # 注意：SQL中使用 -- 作为注释符号，而不是 #
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS students (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT CHECK (age > 0),  -- 年龄必须大于0（使用--作为注释）
                score NUMERIC(5, 2)       -- 成绩（保留2位小数）
            )
        """
        cursor.execute(create_table_sql)
        conn.commit()  # 提交建表操作
        print("✅ 表 'students' 创建/验证成功")

        # 4. 插入一条测试数据
        insert_sql = """
            INSERT INTO students (name, age, score)
            VALUES (%s, %s, %s)
        """
        student = ("小明", 18, 95.5)  # 姓名、年龄、成绩
        cursor.execute(insert_sql, student)
        conn.commit()  # 提交插入操作
        print(f"✅ 插入数据成功：{student}")

        # 5. 查询表中所有数据
        cursor.execute("SELECT * FROM students")
        rows = cursor.fetchall()  # 获取所有结果

        print("\n📋 表中数据：")
        for row in rows:
            # 输出格式：ID | 姓名 | 年龄 | 成绩
            print(f"ID: {row[0]}, 姓名: {row[1]}, 年龄: {row[2]}, 成绩: {row[3]}")

    except Exception as e:
        print(f"❌ 操作失败：{e}")
        conn.rollback()  # 出错时回滚事务
    finally:
        # 6. 关闭连接
        if conn:
            conn.close()
            print("\n🔌 数据库连接已关闭")

if __name__ == "__main__":
    main()
