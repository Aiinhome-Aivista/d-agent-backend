import os
import json
import pandas as pd
import pymysql
from database.db_connection import get_db_connection
from database.config import MYSQL_CONFIG


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "uploads"))


def sync_csv_to_user_db(user_id, connection_id, session_id):

    try:

        conn = get_db_connection()
        cursor = conn.cursor()

        imported_files = []   # ✅ imported file track 

        # fetch credential
        query = """
        SELECT dc.credential
        FROM database_credential dc
        JOIN connection_history ch
        ON dc.session_id = ch.session_id
        WHERE ch.id = %s
        AND dc.user_id = %s
        AND dc.session_id = %s
        """

        cursor.execute(query, (connection_id, user_id, session_id))
        result = cursor.fetchone()



        if not result:
            return {"status": "error", "message": "Credential not found"}

        credential = json.loads(result["credential"])
        files = credential.get("files", [])

        print("FILES FROM DB:", files)

        # fetch user db
        cursor.execute("SELECT name,new_user_db FROM users WHERE id=%s", (user_id,))
        user_data = cursor.fetchone()

        username = user_data["name"]
        user_db = user_data["new_user_db"]

        print("User DB:", user_db)

        # connect user database
        user_conn = pymysql.connect(
            host=MYSQL_CONFIG["host"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
            database=user_db,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )

        user_cursor = user_conn.cursor()

        files_in_folder = os.listdir(UPLOAD_DIR)
        print("FILES IN UPLOAD FOLDER:", files_in_folder)

        for file in files:

            matched_file = None

            for f in files_in_folder:
                if f.lower() == file.lower():
                    matched_file = f
                    break

            if not matched_file:
                print("File not found:", file)
                continue

            file_path = os.path.join(UPLOAD_DIR, matched_file)

            print("Processing file:", file_path)



            df = pd.read_csv(file_path)

            if df.empty:
                print("CSV file empty:", file)
                continue

            df.columns = df.columns.str.strip().str.replace(" ", "_")

            table_name = file.replace(".csv", "").lower()

            # create table
            cols = ", ".join([f"`{c}` TEXT" for c in df.columns])
            create_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols})"

            user_cursor.execute(create_query)

            # insert data
            columns = ", ".join([f"`{c}`" for c in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"""
            INSERT INTO `{table_name}` ({columns})
            VALUES ({placeholders})
            """

            data = [tuple(row) for row in df.values]

            user_cursor.executemany(insert_query, data)

            rows = len(df)

            print(f"Inserted {rows} rows into {table_name}")

            # ✅ imported file track
            imported_files.append(file)

            # insert log
            log_query = """
            INSERT INTO external_db_sync_log
            (user_id,username,external_database,table_name,
            action_type,rows_affected,session_id,new_user_db)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """

            cursor.execute(log_query, (
                user_id,
                username,
                "csv_upload",
                table_name,
                "IMPORT",
                rows,
                session_id,
                user_db
            ))

        return {
            "message": "csv imported successfully",
            "status": "success",
            "imported_files": imported_files
        }

    except Exception as e:

        print("ERROR:", str(e))

        return {
            "status": "error",
            "message": str(e)
        }

