import pymysql
from database.config import MYSQL_CONFIG


def create_user_database(email):

    username = email.split("@")[0]   
    db_name = f"user_{username}_db"

    # Connect without selecting specific database
    connection = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

    try:
        with connection.cursor() as cursor:

            # Check if database already exists
            cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
            result = cursor.fetchone()

            if result:
                return {
                    "status": "exists",
                    "message": "Database already exists for this user",
                    "db_name": db_name
                }

            # Create database if not exists
            cursor.execute(f"CREATE DATABASE `{db_name}`")

            return {
                "status": "created",
                "message": "User database created successfully",
                "db_name": db_name
            }

    finally:
        connection.close()