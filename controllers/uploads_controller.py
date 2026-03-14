import re
import os
import uuid
import json
import pandas as pd
from tqdm import tqdm
from flask import request, jsonify
from urllib.parse import quote_plus
from sqlalchemy import create_engine , text
from apscheduler.schedulers.background import BackgroundScheduler


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "..", "uploads")
UPLOAD_DIR = os.path.abspath(UPLOAD_DIR)

os.makedirs(UPLOAD_DIR, exist_ok=True)
scheduler = BackgroundScheduler()

if not scheduler.running:
    scheduler.start()

# =========================================================
# 1. HELPER: DETECTOR & PARSERS
# =========================================================
def detect_sql_dialect(sql_content):
    sample = sql_content[:50000].upper()
    scores = {"mysql": 0, "mssql": 0, "postgresql": 0}

    if "MYSQLDUMP" in sample or "ENGINE=INNODB" in sample or "`" in sample: scores["mysql"] += 3
    if "[DBO]." in sample or "SET ANSI_NULLS ON" in sample or "\nGO\n" in sample: scores["mssql"] += 3
    if "PG_DUMP" in sample or "PUBLIC." in sample: scores["postgresql"] += 3

    best_match = max(scores, key=scores.get)
    return best_match if scores[best_match] > 0 else "unknown"

def parse_mysql_or_pg(sql_content):
    commands = sql_content.split(';')
    return [cmd.strip() for cmd in commands if cmd.strip() and not cmd.strip().startswith('--')]

def parse_mssql(sql_content):
    commands = re.split(r'(?i)^\s*GO\s*$', sql_content, flags=re.MULTILINE)
    return [cmd.strip() for cmd in commands if cmd.strip() and not cmd.strip().startswith('--')]


def merge_chunks(folder, filename):

    if not filename.endswith(".csv"):
        filename = filename + ".csv"

    merged_path = os.path.join(UPLOAD_DIR, filename)

    parts = sorted(
        os.listdir(folder),
        key=lambda x: int(x.split(".")[0])
    )

    with open(merged_path, "wb") as outfile:
        for part in parts:
            part_path = os.path.join(folder, part)

            with open(part_path, "rb") as infile:
                outfile.write(infile.read())

    return merged_path
# =========================================================
# 2. MAIN CONTROLLER
# =========================================================
def upload_universal_dump_controller(get_db_connection):
    session_id = request.form.get('session_id')
    connection_id = request.form.get('connection_id')

    if not session_id or not connection_id:
        return jsonify({"status": "error", "statuscode": 400, "message": "session_id and connection_id are required"}), 400

    if 'file' not in request.files:
        return jsonify({"status": "error", "statuscode": 400, "message": "No file uploaded"}), 400

    file = request.files['file']
    main_conn = None
    main_cursor = None
    
    try:
        sql_content = file.read().decode('utf-8')
        file_dialect = detect_sql_dialect(sql_content)

        # --- STEP 1: Fetch Credentials & User ID ---
        main_conn = get_db_connection()
        main_cursor = main_conn.cursor(dictionary=True)
        
        # PERFECTLY MATCHED TO YOUR database_credential TABLE
        query = "SELECT `user_id`, `db_type`, `credential` FROM `database_credential` WHERE `connection_id` = %s AND `session_id` = %s"
        main_cursor.execute(query, (connection_id, session_id))
        
        target_db = main_cursor.fetchone()
        
        if not target_db:
            main_cursor.close()
            main_conn.close()
            return jsonify({"status": "error", "statuscode": 404, "message": "Database connection not found."}), 404

        target_db_type = target_db['db_type']
        user_id = target_db['user_id']
        creds = json.loads(target_db['credential'])
        
        db_user = creds.get('user') or creds.get('username') or ''
        db_pass = creds.get('password') or creds.get('pwd') or ''
        db_host = creds.get('host') or creds.get('server') or 'localhost'
        db_name = creds.get('database') or creds.get('dbname') or ''
        
        # Safely get connection name or generate a fallback
        conn_name = creds.get('connection_name') or f"External {target_db_type.upper()} DB"

        # --- STEP 2: The Safety Gatekeeper ---
        if file_dialect != "unknown" and file_dialect != target_db_type:
             main_cursor.close()
             main_conn.close()
             return jsonify({
                 "status": "error", 
                 "statuscode": 400, 
                 "message": f"Conflict: Uploaded {file_dialect.upper()} file, but target is a {target_db_type.upper()} database."
             }), 400

        # --- STEP 3: Connect & Execute ---
        executed_count = 0

        if target_db_type == 'mysql':
            import pymysql
            ext_conn = pymysql.connect(
                host=db_host, port=int(creds.get('port') or 3306),
                user=db_user, password=db_pass, database=db_name
            )
            ext_cursor = ext_conn.cursor()
            commands = parse_mysql_or_pg(sql_content)
            for cmd in commands: ext_cursor.execute(cmd)
            ext_conn.commit()
            ext_cursor.close(); ext_conn.close()
            executed_count = len(commands)

        elif target_db_type == 'mssql':
            import pyodbc
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_host},{creds.get('port') or 1433};DATABASE={db_name};UID={db_user};PWD={db_pass}"
            ext_conn = pyodbc.connect(conn_str)
            ext_cursor = ext_conn.cursor()
            commands = parse_mssql(sql_content)
            for cmd in commands: ext_cursor.execute(cmd)
            ext_conn.commit()
            ext_cursor.close(); ext_conn.close()
            executed_count = len(commands)

        elif target_db_type == 'postgresql':
            import psycopg2
            ext_conn = psycopg2.connect(
                host=db_host, port=creds.get('port') or 5432,
                user=db_user, password=db_pass, dbname=db_name
            )
            ext_cursor = ext_conn.cursor()
            commands = parse_mysql_or_pg(sql_content)
            for cmd in commands: ext_cursor.execute(cmd)
            ext_conn.commit()
            ext_cursor.close(); ext_conn.close()
            executed_count = len(commands)

        # --- STEP 4: Log Success to connection_history ---
        # PERFECTLY MATCHED TO YOUR connection_history TABLE
        log_query = """
            INSERT INTO `connection_history` 
            (`user_id`, `connection_name`, `db_type`, `target_host`, `status`, `session_id`) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        history_name = f"Uploaded Dump: {file.filename} to {conn_name}"
        
        main_cursor.execute(log_query, (
            user_id,             # from database_credential
            history_name,        # dynamically generated name
            'sql_upload',        # db_type indicator
            db_host,             # target_host
            'Success',           # status
            session_id           # session_id
        ))
        main_conn.commit()
        
        main_cursor.close()
        main_conn.close()

        return jsonify({
            "status": "success",
            "statuscode": 200,
            "message": f"Successfully executed {executed_count} commands on {conn_name}."
        }), 200

    except Exception as e:
        # If execution fails, we can optionally log the failure to connection_history here too!
        if main_conn and main_cursor:
            main_cursor.close()
            main_conn.close()
            
        return jsonify({
            "status": "error", 
            "statuscode": 500, 
            "message": f"Execution failed: {str(e)}"
        }), 500


CHUNK_DIR = os.path.join(BASE_DIR, "..", "chunk_uploads")
CHUNK_DIR = os.path.abspath(CHUNK_DIR)
os.makedirs(CHUNK_DIR, exist_ok=True)


def upload_chunk_controller(get_db_connection):

    chunk = request.files.get("chunk")
    chunk_index = request.form.get("chunk_index")
    total_chunks = request.form.get("total_chunks")
    session_id = request.form.get("session_id")
    filename = request.form.get("filename")

    if not chunk:
        return jsonify({"status":"error","message":"chunk missing"}),400

    session_folder = os.path.join(CHUNK_DIR, session_id)
    os.makedirs(session_folder, exist_ok=True)

    chunk_path = os.path.join(session_folder, f"{chunk_index}.part")
    chunk.save(chunk_path)

    uploaded = len([f for f in os.listdir(session_folder) if f.endswith(".part")])

    if uploaded == int(total_chunks):

        merged_path = merge_chunks(session_folder, filename)

        print(f"Merged file ready: {merged_path}")

        user_id = request.form.get("user_id")
        session_id = request.form.get("session_id")

        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT new_user_db
            FROM users
            WHERE id=%s
        """, (user_id,))

        user_data = cursor.fetchone()
        allocated_db_name = user_data["new_user_db"]

        # 🔹 INSERT INTO connection_history
        history_name = f"Chunk Upload: {filename} to allocated DB ({allocated_db_name})"

        cursor.execute("""
            INSERT INTO connection_history
            (user_id, session_id, connection_name, db_type, target_host, status)
            VALUES (%s, %s, %s, 'csv_chunk_upload', %s, 'Success')
        """, (user_id, session_id, history_name, "72.61.226.68"))

        cred_json = json.dumps({"files":[filename]})

        cursor.execute("""
            INSERT INTO database_credential
            (user_id, session_id, db_type, credential)
            VALUES (%s,%s,'csv_upload',%s)
        """,(user_id,session_id,cred_json))


        db_conn.commit()

        cursor.close()
        db_conn.close()

        scheduler.add_job(
            func=process_csv_job,
            args=[[merged_path], allocated_db_name, "72.61.226.68", "aiinhome", "Aiin@2026", 3306],
            trigger='date',
            id=str(uuid.uuid4()),
            replace_existing=True
        )

        import shutil
        shutil.rmtree(session_folder)

    return jsonify({
        "status":"success",
        "chunk_index":chunk_index
    })

# =========================================================
# FINAL: Handles the /upload_csv route (POST)
# 1. Validates user against workspace_users table
# 2. Streams huge CSVs directly into User-Allocated Database
# =========================================================
def upload_csv_controller(get_db_connection):

    user_id = request.form.get('user_id')
    session_id = request.form.get('session_id')

    if not user_id or not session_id:
        return jsonify({"status": "error", "message": "user_id and session_id required"}), 400

    uploaded_files = request.files.getlist('files')

    try:

        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)

        # Validate workspace
        cursor.execute("""
            SELECT user_id
            FROM workspace_users
            WHERE session_id=%s AND user_id=%s
        """, (session_id, user_id))

        if not cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": "Access denied for workspace"
            }), 403

        # Fetch user DB
        cursor.execute("""
            SELECT new_user_db
            FROM users
            WHERE id=%s
        """, (user_id,))

        user_data = cursor.fetchone()

        if not user_data:
            return jsonify({"status":"error","message":"User DB missing"}),404

        allocated_db_name = user_data["new_user_db"]

        # DB server credentials
        db_user = "aiinhome"
        db_pass = "Aiin@2026"
        db_host = "72.61.226.68"
        db_port = 3306

        # Save uploaded files
        saved_paths = []
        original_filenames = []

        for file in uploaded_files:
            original_filenames.append(file.filename)
            
            filename = f"{uuid.uuid4()}_{file.filename}"
            path = os.path.join(UPLOAD_DIR, filename)

            file.save(path)
            saved_paths.append(path)

        # Schedule background processing
        job = scheduler.add_job(
            func=process_csv_job,
            args=[saved_paths, allocated_db_name, db_host, db_user, db_pass, db_port],
            trigger='date',
            id=str(uuid.uuid4()),
            replace_existing=True
        )

        print(f"Scheduled CSV processing job: {job.id}")

        # =========================================================
        # 3. INSERT HISTORY INTO DATABASE RIGHT HERE
        # =========================================================
        try:
            import json
            history_name = f"Uploaded {len(uploaded_files)} CSV(s) to allocated DB ({allocated_db_name})"
            
            # Save the main history row
            cursor.execute("""
                INSERT INTO connection_history (user_id, session_id, connection_name, db_type, target_host, status)
                VALUES (%s, %s, %s, 'csv_upload', %s, 'Success')
            """, (user_id, session_id, history_name, db_host))
            
            # Save the ACTUAL FILE NAMES into the credential JSON column
            cred_json = json.dumps({"files": original_filenames})
            cursor.execute("""
                INSERT INTO database_credential
                (user_id, connection_id, session_id, db_type, credential)
                VALUES (%s,%s,%s,'csv_upload',%s)
            """, (user_id, session_id, cred_json))
            
            db_conn.commit()
        except Exception as log_err:
            print(f"Error logging CSV history: {log_err}")
        cursor.close()
        db_conn.close()

        return jsonify({
            "status": "accepted",
            "message": "CSV upload scheduled for processing",
            "files": len(saved_paths)
        }), 200

    except Exception as e:
        return jsonify({
            "status":"error",
            "message":str(e)
        }),500

def process_csv_job(file_paths, allocated_db_name, db_host, db_user, db_pass, db_port):

    safe_user = quote_plus(db_user)
    safe_pass = quote_plus(db_pass)

    base_url = f"mysql+pymysql://{safe_user}:{safe_pass}@{db_host}:{db_port}"

    target_engine = create_engine(
        f"{base_url}/{allocated_db_name}",
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600
    )

    tables_created = []

    for path in file_paths:

        print(f"\n🚀 Starting processing for file: {path}")

        # Count total rows (for progress calculation)
        print("🔎 Counting rows...")
        with open(path, encoding="latin1") as f:
            total_rows = sum(1 for _ in f)

        print(f"📊 Total rows detected: {total_rows}")

        raw_name = path.split("/")[-1].rsplit('.', 1)[0]
        table_name = "".join([c if c.isalnum() else "_" for c in raw_name]).lower()[:60]

        first_chunk = True
        chunk_size = 300000
        processed_rows = 0

        for chunk in pd.read_csv(
            path,
            chunksize=chunk_size,
            dtype=str,
            encoding="latin1",
            engine="python",
            on_bad_lines="skip",
            memory_map=True
        ):

            processed_rows += len(chunk)
            percent = (processed_rows / total_rows) * 100

            print(
                f"[PROGRESS] {table_name} → "
                f"{percent:.2f}% "
                f"({processed_rows}/{total_rows} rows)"
            )

            chunk.columns = [
                "".join([c if c.isalnum() else "_" for c in col]).lower()
                for col in chunk.columns
            ]

            if first_chunk:
                chunk.to_sql(
                    table_name,
                    target_engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                first_chunk = False
            else:
                chunk.to_sql(
                    table_name,
                    target_engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

        tables_created.append(table_name)

        print(f"✅ Finished loading table: {table_name}")

    print("\n🎉 All files processed successfully!")

    return tables_created
    