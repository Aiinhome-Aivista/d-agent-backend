import json
import pymysql
from database.config import MYSQL_CONFIG


def sync_external_database(user_id, connection_id, session_id):

    # fetch credential from database_credential table
    cred_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with cred_conn.cursor() as cursor:

        print("INPUT VALUES →", user_id, connection_id, session_id)

        cursor.execute("SELECT user_id, connection_id, session_id FROM database_credential")
        print("DB ROWS →", cursor.fetchall())

        cursor.execute("""
        SELECT credential
        FROM database_credential
        WHERE user_id=%s AND connection_id=%s AND session_id=%s
        """, (user_id, connection_id, session_id))

        result = cursor.fetchone()

        print("QUERY RESULT →", result)

        if not result:
            raise Exception("Database credential not found")

        external_db = json.loads(result[0])

    cred_conn.close()

    required_fields = ["host", "port","username", "password", "database"]
    for field in required_fields:
        if field not in external_db or not external_db[field]:
            raise Exception(f"Missing external DB field: {field}")

    # get username from users table
    user_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with user_conn.cursor() as cursor:
        cursor.execute("SELECT email, new_user_db  FROM users WHERE id=%s", (user_id,))
        result = cursor.fetchone()

        if not result:
            raise Exception("User not found")
        
        email = result[0]
        new_user_db = result[1] 
        username = email.split("@")[0]

    user_conn.close()

    user_db_name = new_user_db

    # check if first sync
    log_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with log_conn.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*)
            FROM external_db_sync_log
            WHERE username=%s AND external_database=%s AND session_id=%s
        """, (username, external_db["database"], session_id))

        sync_count = cursor.fetchone()[0]

    log_conn.close()

    first_sync = sync_count == 0

    source_conn = pymysql.connect(
        host=external_db["host"],
        port=external_db.get("port", 3306),
        user=external_db["username"],
        password=external_db["password"],
        database=external_db["database"],
        autocommit=True
    )

    target_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=user_db_name,
        autocommit=True
    )

    new_tables = []
    updated_tables = []
    situations = []
    table_summary = []
    total_rows = 0
    total_columns = 0

    try:
        with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:

            log_query = """
            INSERT INTO TraverseAi_dis.external_db_sync_log
            (user_id, new_user_db ,username, external_database, table_name, action_type, rows_affected, session_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """

            source_cursor.execute("SHOW TABLES")
            source_tables = [t[0] for t in source_cursor.fetchall()]

            target_cursor.execute("SHOW TABLES")
            target_tables = [t[0] for t in target_cursor.fetchall()]

            # Disable FK
            target_cursor.execute("SET FOREIGN_KEY_CHECKS=0")

            for table_name in source_tables:
                # count rows
                source_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = source_cursor.fetchone()[0]

                # count columns
                source_cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                columns = source_cursor.fetchall()
                col_count = len(columns)

                total_rows += row_count
                total_columns += col_count

                table_summary.append({
                    "table": f"{external_db['database']}.{table_name}",
                    "rows": row_count,
                    "columns": col_count
                })

                new_table_name = f"{external_db['database']}_{table_name}"

                # ---------- NEW TABLE ----------
                # if new_table_name not in target_tables:

                #     source_cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                #     create_query = source_cursor.fetchone()[1]

                #     create_query = create_query.replace(
                #         f"CREATE TABLE `{table_name}`",
                #         f"CREATE TABLE `{new_table_name}`"
                #     )

                #     target_cursor.execute(create_query)

                #     source_cursor.execute(f"SELECT * FROM `{table_name}`")
                #     rows = source_cursor.fetchall()

                #     inserted_count = 0

                #     if rows:
                #         placeholders = ", ".join(["%s"] * len(rows[0]))

                #         insert_query = f"""
                #         INSERT INTO `{new_table_name}`
                #         VALUES ({placeholders})
                #         """

                #         target_cursor.executemany(insert_query, rows)
                #         inserted_count = len(rows)

                #     new_tables.append({
                #         "table": new_table_name,
                #         "rows_inserted": inserted_count
                #     })
                #     if not first_sync:
                #         situations.append({
                #             "type": "NEW_DATASET",
                #             "table": new_table_name,
                #             "message": f"I see there are new data sources available: {new_table_name}. Do you want to add them?",
                #             "buttons": ["Yes", "No"]
                #         })
               
                #     target_cursor.execute(
                #         log_query,
                #         (username, external_db["database"], new_table_name, "NEW_TABLE", inserted_count)
                #     )

                # ---------- NEW TABLE ----------
                if new_table_name not in target_tables:

                    if first_sync:
                        new_tables.append(new_table_name)

                        source_cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                        create_query = source_cursor.fetchone()[1]

                        create_query = create_query.replace(
                            f"CREATE TABLE `{table_name}`",
                            f"CREATE TABLE `{new_table_name}`"
                        )

                        target_cursor.execute(create_query)

                        source_cursor.execute(f"SELECT * FROM `{table_name}`")
                        rows = source_cursor.fetchall()

                        inserted_count = 0

                        if rows:
                            placeholders = ", ".join(["%s"] * len(rows[0]))

                            insert_query = f"""
                            INSERT INTO `{new_table_name}`
                            VALUES ({placeholders})
                            """

                            target_cursor.executemany(insert_query, rows)
                            inserted_count = len(rows)

                        #  LOG INSERT
                        target_cursor.execute(
                            log_query,
                            (   user_id,
                                new_user_db,
                                username,
                                external_db["database"],
                                new_table_name,
                                "NEW_TABLE",
                                inserted_count,
                                session_id
                            )
                        )

                    else:
                        situations.append({
                            "type": "NEW_DATASET",
                            "table": new_table_name,
                            "message": f"I see there are new data sources available: {new_table_name}. Do you want to add them?",
                            "buttons": ["Yes", "No"]
                        })

                else:

                    # check new columns
                    source_cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                    source_columns = [c[0] for c in source_cursor.fetchall()]

                    target_cursor.execute(f"SHOW COLUMNS FROM `{new_table_name}`")
                    target_columns = [c[0] for c in target_cursor.fetchall()]

                    new_columns = set(source_columns) - set(target_columns)
                
                    if new_columns and not first_sync:
                        if not any(s["table"] == new_table_name and s["type"]=="SCHEMA_CHANGE" for s in situations):
                            situations.append({
                                "type": "SCHEMA_CHANGE",
                                "table": new_table_name,
                                "message": f"New columns detected in {new_table_name}: {', '.join(new_columns)}",
                                "buttons": ["Yes", "No"]
                            })


                    source_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                    source_count = source_cursor.fetchone()[0]

                    target_cursor.execute(f"SELECT COUNT(*) FROM `{new_table_name}`")
                    target_count = target_cursor.fetchone()[0]

                    if source_count > target_count and not first_sync:
                        if not any(s["table"] == new_table_name for s in situations):
                            situations.append({
                                "type": "DATA_DISCREPANCY",
                                "table": new_table_name,
                                "message": f"I found some discrepancy in table {new_table_name}. I'm doing reconciliation.",
                                "buttons": ["Yes", "No"]
                            })

            data_size_mb = round((total_rows * total_columns * 8) / (1024 * 1024), 2)

            # Enable FK
            target_cursor.execute("SET FOREIGN_KEY_CHECKS=1")

            if not situations and not new_tables:

                target_cursor.execute(
                    log_query,
                    (   user_id,
                        new_user_db,
                        username,
                        external_db["database"],
                        "ALL_TABLES",
                        "NO_CHANGE",
                        0,
                        session_id
                    )
                )

    except Exception as e:

        situations.append({
            "type": "FAILED_ATTEMPT",
            "message": "I see that my last attempt failed. I'm trying again.",
            "buttons": ["Retry", "No"]
        })

        raise e

    finally:
        source_conn.close()
        target_conn.close()

    return {
        "summary": {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "data_size_mb": data_size_mb,
            "last_sync": "Just now"
        },
        "tables": table_summary,
        "situations": situations,
        "new_tables": new_tables
    }

 
def apply_external_sync(user_id, connection_id, session_id, table):

    # fetch credential from database_credential table
    cred_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with cred_conn.cursor() as cursor:
        cursor.execute("""
            SELECT credential
            FROM database_credential
            WHERE user_id=%s AND connection_id=%s AND session_id=%s
        """, (user_id, connection_id, session_id))

        result = cursor.fetchone()

        if not result:
            raise Exception("Database credential not found")

        external_db = json.loads(result[0])

    cred_conn.close()



    # get username from users table
    user_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with user_conn.cursor() as cursor:
        cursor.execute("SELECT email, new_user_db  FROM users WHERE id=%s", (user_id,))
        result = cursor.fetchone()

        if not result:
            raise Exception("User not found")

        email = result[0]
        new_user_db = result[1] 
        username = email.split("@")[0]

    user_conn.close()

    user_db_name = new_user_db

    # ensure user database exists
    db_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"]
    )

    with db_conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{user_db_name}`")

    db_conn.close()

    source_conn = pymysql.connect(
        host=external_db["host"],
        port=external_db.get("port", 3306),
        user=external_db["username"],
        password=external_db["password"],
        database=external_db["database"],
        autocommit=True
    )

    target_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=user_db_name,
        autocommit=True
    )

    try:
        with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:

            # external table name
            original_table = table.replace(f"{external_db['database']}_", "")

            # check if table exists in target database
            target_cursor.execute("SHOW TABLES LIKE %s", (table,))
            exists = target_cursor.fetchone()

            # if table does not exist → create it
            if not exists:

                source_cursor.execute(f"SHOW CREATE TABLE `{original_table}`")
                create_query = source_cursor.fetchone()[1]

                create_query = create_query.replace(
                    f"CREATE TABLE `{original_table}`",
                    f"CREATE TABLE `{table}`"
                )

                target_cursor.execute(create_query)

                # copy all rows
                source_cursor.execute(f"SELECT * FROM `{original_table}`")
                rows = source_cursor.fetchall()

                if rows:
                    placeholders = ", ".join(["%s"] * len(rows[0]))

                    insert_query = f"""
                    INSERT INTO `{table}`
                    VALUES ({placeholders})
                    """

                    target_cursor.executemany(insert_query, rows)

                return

            source_cursor.execute(f"SELECT COUNT(*) FROM `{original_table}`")
            source_count = source_cursor.fetchone()[0]

            target_cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            target_count = target_cursor.fetchone()[0]

            if source_count > target_count:

                offset = target_count

                source_cursor.execute(
                    f"SELECT * FROM `{original_table}` LIMIT {offset}, {source_count-offset}"
                )

                rows = source_cursor.fetchall()

                if rows:

                    placeholders = ", ".join(["%s"] * len(rows[0]))

                    insert_query = f"""
                    INSERT INTO `{table}`
                    VALUES ({placeholders})
                    """

                    target_cursor.executemany(insert_query, rows)

    finally:
        source_conn.close()
        target_conn.close()

def apply_bulk_external_sync(user_id, connection_id, session_id, tables, action):
    """
    Service to handle multiple tables at once and support Replace All vs Update Existing.
    """
    # 1. Fetch credential from database_credential table
    cred_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with cred_conn.cursor() as cursor:
        cursor.execute("""
            SELECT credential
            FROM database_credential
            WHERE user_id=%s AND connection_id=%s AND session_id=%s
        """, (user_id, connection_id, session_id))

        result = cursor.fetchone()
        if not result:
            raise Exception("Database credential not found")
        external_db = json.loads(result[0])
    cred_conn.close()

    # 2. Get username from users table to find target database
    user_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database="TraverseAi_dis"
    )

    with user_conn.cursor() as cursor:
        cursor.execute("SELECT email, new_user_db FROM users WHERE id=%s", (user_id,))
        result = cursor.fetchone()
        if not result:
            raise Exception("User not found")

        email = result[0]
        new_user_db = result[1]
        username = email.split("@")[0]
    user_conn.close()

    user_db_name = f"user_{username}_db"

    # 3. Connect to Source and Target Databases
    source_conn = pymysql.connect(
        host=external_db["host"],
        port=external_db.get("port", 3306),
        user=external_db["username"],
        password=external_db["password"],
        database=external_db["database"],
        autocommit=True
    )

    target_conn = pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=user_db_name,
        autocommit=True
    )

    try:
        with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:
            
            # Loop through all tables sent from the UI
            for table in tables:
                # original external table name without the DB prefix
                original_table = table.replace(f"{external_db['database']}_", "")

                # ---------------------------------------------------------
                # LOGIC FOR "Replace All"
                # ---------------------------------------------------------
                if action == "replace":
                    # Drop the table if it exists to start fresh
                    target_cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                    
                    # Get create schema from source
                    source_cursor.execute(f"SHOW CREATE TABLE `{original_table}`")
                    create_query = source_cursor.fetchone()[1]
                    create_query = create_query.replace(
                        f"CREATE TABLE `{original_table}`",
                        f"CREATE TABLE `{table}`"
                    )
                    target_cursor.execute(create_query)

                    # Copy all rows
                    source_cursor.execute(f"SELECT * FROM `{original_table}`")
                    rows = source_cursor.fetchall()

                    if rows:
                        placeholders = ", ".join(["%s"] * len(rows[0]))
                        insert_query = f"INSERT INTO `{table}` VALUES ({placeholders})"
                        target_cursor.executemany(insert_query, rows)

                # ---------------------------------------------------------
                # LOGIC FOR "Update Existing" (Append / Create New)
                # ---------------------------------------------------------
                elif action == "update":
                    # check if table exists in target database
                    target_cursor.execute("SHOW TABLES LIKE %s", (table,))
                    exists = target_cursor.fetchone()

                    if not exists:
                        # Table doesn't exist, create and copy all
                        source_cursor.execute(f"SHOW CREATE TABLE `{original_table}`")
                        create_query = source_cursor.fetchone()[1]
                        create_query = create_query.replace(
                            f"CREATE TABLE `{original_table}`",
                            f"CREATE TABLE `{table}`"
                        )
                        target_cursor.execute(create_query)

                        source_cursor.execute(f"SELECT * FROM `{original_table}`")
                        rows = source_cursor.fetchall()

                        if rows:
                            placeholders = ", ".join(["%s"] * len(rows[0]))
                            insert_query = f"INSERT INTO `{table}` VALUES ({placeholders})"
                            target_cursor.executemany(insert_query, rows)
                    else:
                        # Table exists, append only new rows
                        source_cursor.execute(f"SELECT COUNT(*) FROM `{original_table}`")
                        source_count = source_cursor.fetchone()[0]

                        target_cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                        target_count = target_cursor.fetchone()[0]

                        if source_count > target_count:
                            offset = target_count
                            source_cursor.execute(
                                f"SELECT * FROM `{original_table}` LIMIT {offset}, {source_count-offset}"
                            )
                            rows = source_cursor.fetchall()

                            if rows:
                                placeholders = ", ".join(["%s"] * len(rows[0]))
                                insert_query = f"INSERT INTO `{table}` VALUES ({placeholders})"
                                target_cursor.executemany(insert_query, rows)

    finally:
        source_conn.close()
        target_conn.close()
