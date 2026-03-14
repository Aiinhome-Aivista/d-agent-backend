# # controllers/session_analysis_controller.py
# #
# # POST /session-analysis
# #
# # Body:
# # {
# #   "session_id": "xxx",
# #   "topics":     ["AI", "Python"],          // optional — from saved_web_results
# #   "databaseses":  ["recipe", "code_complacity"]  // optional — from external_db_sync_log
# # }
# #
# # At least one of topics / databaseses must be provided.
# #
# # Returns: structured multi-section analysis report as JSON

# import re
# import json
# import requests
# import mysql.connector
# from flask import request, jsonify
# from database.config import MISTRAL_API_KEY, MISTRAL_MODEL, MYSQL_CONFIG

# from pyvis.network import Network
# import os
# import uuid
# from database.config import GRAPH_FOLDER, BASE_URL

# MISTRAL_URL    = "https://api.mistral.ai/v1/chat/completions"
# MAX_ROWS       = 100
# MAX_CTX_CHARS  = 24000




# # def generate_session_graph(session_id, web_data, db_data):

# #     net = Network(
# #         height="850px",
# #         width="100%",
# #         bgcolor="#222222",
# #         font_color="white",
# #         directed=False
# #     )

# #     SESSION_STYLE = {
# #         "color": "#00bfa5",
# #         "size": 35
# #     }

# #     TOPIC_STYLE = {
# #         "color": "#ff4081",
# #         "size": 20
# #     }

# #     DB_STYLE = {
# #         "color": "#2979ff",
# #         "size": 25
# #     }

# #     TABLE_STYLE = {
# #         "color": "#ffc107",
# #         "size": 15
# #     }

# #     session_node = f"session_{session_id}"

# #     net.add_node(session_node, label=f"Session {session_id}", **SESSION_STYLE)

# #     # Web Topics
# #     for w in web_data:

# #         topic = w["topic"]

# #         net.add_node(topic, label=topic, **TOPIC_STYLE)
# #         net.add_edge(session_node, topic)

# #         for item in w["items"]:
# #             title = item["title"][:40]

# #             net.add_node(title, label=title)
# #             net.add_edge(topic, title)

# #     # databaseses
# #     for db in db_data:

# #         db_name = db["external_database"]

# #         net.add_node(db_name, label=db_name, **DB_STYLE)
# #         net.add_edge(session_node, db_name)

# #         for table in db["tables"]:

# #             tname = table["table_name"]

# #             net.add_node(tname, label=tname, **TABLE_STYLE)
# #             net.add_edge(db_name, tname)

# #     html_filename = f"graph_{uuid.uuid4().hex[:8]}.html"

# #     html_path = os.path.join(GRAPH_FOLDER, html_filename)

# #     net.save_graph(html_path)

# #     graph_url = f"{BASE_URL}/graphs/{html_filename}"

# #     return graph_url



# def detect_table_relationships(table_columns):

#     prompt = f"""
# You are a database expert.

# Find relationships between tables using column names.

# Return ONLY JSON like:
# [
#   {{"table1":"table_name","column1":"column","table2":"table_name","column2":"column"}}
# ]

# Tables and columns:
# {json.dumps(table_columns, indent=2)}
# """

#     try:
#         resp = requests.post(
#             MISTRAL_URL,
#             headers={
#                 "Authorization": f"Bearer {MISTRAL_API_KEY}",
#                 "Content-Type": "application/json"
#             },
#             json={
#                 "model": MISTRAL_MODEL,
#                 "messages":[{"role":"user","content":prompt}],
#                 "temperature":0
#             },
#             timeout=60
#         )

#         text = resp.json()["choices"][0]["message"]["content"]

#         return json.loads(text)

#     except Exception as e:
#         print("Relationship detection error:", e)
#         return []


# def generate_session_graph(session_id, web_data, db_data):

#     net = Network(
#         height="850px",
#         width="100%",
#         bgcolor="#222222",
#         font_color="white",
#         directed=False
#     )

#     SESSION_STYLE = {"color": "#00bfa5", "size": 35}
#     TOPIC_STYLE   = {"color": "#ff4081", "size": 20}
#     DB_STYLE      = {"color": "#2979ff", "size": 25}
#     TABLE_STYLE   = {"color": "#ffc107", "size": 15}

#     session_node = f"session_{session_id}"
#     net.add_node(session_node, label=f"Session {session_id}", **SESSION_STYLE)

#     # -------------------------
#     # Web Topics
#     # -------------------------
#     for w in web_data:

#         topic = w["topic"]
#         net.add_node(topic, label=topic, **TOPIC_STYLE)
#         net.add_edge(session_node, topic)

#         for item in w["items"]:
#             title = item["title"][:40]
#             net.add_node(title, label=title)
#             net.add_edge(topic, title)


#     # -------------------------
#     # DB SECTION
#     # -------------------------
#     table_columns = {} 
#     for db in db_data:

#         db_name = db["external_database"]

#         net.add_node(db_name, label=db_name, **DB_STYLE)
#         net.add_edge(session_node, db_name)

#         for table in db["tables"]:

#             tname = table["table_name"]
#             columns = table.get("columns", [])
#             table_columns[tname] = columns
#             # Table node
#             net.add_node(
#                 tname,
#                 label=tname,
#                 shape="box",
#                 **TABLE_STYLE
#             )

#             net.add_edge(db_name, tname)

#             # Column nodes
#             for col in columns:

#                 col_node = f"{tname}.{col}"

#                 net.add_node(
#                     col_node,
#                     label=col,
#                     color="#9ccc65",
#                     size=10
#                 )

#                 net.add_edge(tname, col_node)


#     # -------------------------
#     # Detect Table Relationships using LLM
#     # -------------------------

#     relationships = detect_table_relationships(table_columns)

#     for r in relationships:

#         try:

#             col1 = f"{r['table1']}.{r['column1']}"
#             col2 = f"{r['table2']}.{r['column2']}"

#             net.add_edge(
#                 col1,
#                 col2,
#                 color="#ff5252",
#                 width=4,
#                 label="relation"
#             )

#         except Exception as e:
#             print("Relationship edge error:", e)

#     # -------------------------
#     # Save Graph
#     # -------------------------
#     html_filename = f"graph_{uuid.uuid4().hex[:8]}.html"
#     html_path = os.path.join(GRAPH_FOLDER, html_filename)

#     net.save_graph(html_path)

#     graph_url = f"{BASE_URL}/graphs/{html_filename}"

#     return graph_url
# # ══════════════════════════════════════════════════════
# # DATA FETCHERS
# # ══════════════════════════════════════════════════════

# def _fetch_web_data(session_id: str, topics: list, conn) -> list:
#     """Fetch saved_web_results filtered by session_id + topic list."""
#     if not topics:
#         return []
#     cursor = None
#     results = []
#     try:
#         cursor = conn.cursor(dictionary=True)
#         placeholders = ",".join(["%s"] * len(topics))
#         cursor.execute(f"""
#             SELECT topic, title, url, brief, saved_at
#             FROM saved_web_results
#             WHERE session_id = %s
#               AND topic IN ({placeholders})
#             ORDER BY topic, saved_at DESC
#         """, [session_id] + topics)
#         rows = cursor.fetchall()

#         # Group by topic
#         grouped = {}
#         for r in rows:
#             t = r["topic"]
#             if t not in grouped:
#                 grouped[t] = []
#             grouped[t].append({
#                 "title":    r["title"],
#                 "url":      r["url"],
#                 "brief":    r.get("brief", ""),
#                 "saved_at": str(r["saved_at"]) if r["saved_at"] else None
#             })

#         for topic, items in grouped.items():
#             results.append({
#                 "source_type":  "web",
#                 "topic":        topic,
#                 "result_count": len(items),
#                 "items":        items
#             })
#     except Exception as e:
#         print(f"[Analysis] web fetch error: {e}")
#     finally:
#         if cursor: cursor.close()
#     return results

# def _fetch_db_data(session_id: str, databaseses: list, conn) -> list:
#     """
#     For each requested database:
#       1. Get new_user_db from external_db_sync_log
#       2. Connect to new_user_db and read all tables (up to MAX_ROWS each)
#     """

#     if not databaseses:
#         return []

#     cursor = None
#     results = []

#     try:
#         cursor = conn.cursor(dictionary=True)

#         placeholders = ",".join(["%s"] * len(databaseses))

#         cursor.execute(f"""
#             SELECT DISTINCT external_database, new_user_db
#             FROM external_db_sync_log
#             WHERE session_id = %s
#               AND external_database IN ({placeholders})
#               AND new_user_db IS NOT NULL
#               AND new_user_db != ''
#         """, [session_id] + databaseses)

#         rows = cursor.fetchall()

#         db_map = {r["external_database"]: r["new_user_db"] for r in rows}

#         print(f"[Analysis] DB MAP -> {db_map}")

#     except Exception as e:
#         print(f"[Analysis] db_map error: {e}")
#         return []

#     finally:
#         if cursor:
#             cursor.close()

#     for ext_db in databaseses:

#         new_db = db_map.get(ext_db)

#         if not new_db:
#             print(f"[Analysis] No mapped DB for {ext_db}")
#             continue

#         db_result = {
#             "source_type": "database",
#             "external_database": ext_db,
#             "new_user_db": new_db,
#             "tables": []
#         }

#         ext_conn = None
#         ext_cur = None

#         try:

#             print(f"[Analysis] Connecting DB -> {new_db}")

#             ext_conn = mysql.connector.connect(
#                 host=MYSQL_CONFIG["host"],
#                 port=MYSQL_CONFIG["port"],
#                 user=MYSQL_CONFIG["user"],
#                 password=MYSQL_CONFIG["password"],
#                 database=new_db,
#                 connection_timeout=10
#             )

#             ext_cur = ext_conn.cursor(dictionary=True)

#             # Fetch tables
#             ext_cur.execute("SHOW TABLES")
#             tables_raw = ext_cur.fetchall()

#             tables = [list(r.values())[0] for r in tables_raw]

#             print(f"[Analysis] Tables in {new_db} -> {tables}")

#             for t in tables:

#                 print(f"[Analysis] Reading table -> {t}")

#                 try:

#                     ext_cur.execute(f"SELECT * FROM `{t}` LIMIT %s", (MAX_ROWS,))
#                     rows = ext_cur.fetchall()

#                     row_count = len(rows)

#                     if row_count > 0:
#                         cols = list(rows[0].keys())
#                     else:
#                         cols = []

#                     # Column statistics
#                     col_stats = {}

#                     if row_count > 0:

#                         for col in cols:

#                             vals = []

#                             for r in rows:
#                                 val = r.get(col)

#                                 if val is None:
#                                     continue

#                                 val_str = str(val).strip()

#                                 if val_str:
#                                     vals.append(val_str)

#                             distinct_vals = list(dict.fromkeys(vals))

#                             col_stats[col] = {
#                                 "total_values": len(vals),
#                                 "distinct_values": len(distinct_vals),
#                                 "sample": distinct_vals[:10]
#                             }

#                     db_result["tables"].append({
#                         "table_name": t,
#                         "row_count": row_count,
#                         "columns": cols,
#                         "column_stats": col_stats,
#                         "sample_rows": rows[:5] if rows else []
#                     })

#                 except Exception as table_error:
#                     print(f"[Analysis] table {t} error -> {table_error}")

#         except Exception as db_error:
#             print(f"[Analysis] connect {new_db} error -> {db_error}")

#         finally:

#             if ext_cur:
#                 try:
#                     ext_cur.close()
#                 except:
#                     pass

#             if ext_conn:
#                 try:
#                     ext_conn.close()
#                 except:
#                     pass

#         results.append(db_result)

#     return results


# # def _fetch_db_data(session_id: str, databaseses: list, conn) -> list:
# #     """
# #     For each requested database:
# #       1. Get new_user_db from external_db_sync_log
# #       2. Connect to new_user_db and read all tables (up to MAX_ROWS each)
# #     """
# #     if not databaseses:
# #         return []
# #     cursor = None
# #     results = []
# #     try:
# #         cursor = conn.cursor(dictionary=True)
# #         placeholders = ",".join(["%s"] * len(databaseses))
# #         cursor.execute(f"""
# #             SELECT DISTINCT external_database, new_user_db
# #             FROM external_db_sync_log
# #             WHERE session_id = %s
# #               AND external_database IN ({placeholders})
# #               AND new_user_db IS NOT NULL AND new_user_db != ''
# #         """, [session_id] + databaseses)
# #         db_map = {r["external_database"]: r["new_user_db"] for r in cursor.fetchall()}
# #     except Exception as e:
# #         print(f"[Analysis] db_map error: {e}")
# #         return []
# #     finally:
# #         if cursor: cursor.close()

# #     for ext_db in databaseses:
# #         new_db = db_map.get(ext_db)
# #         if not new_db or not re.match(r'^\w+$', new_db):
# #             continue

# #         db_result = {
# #             "source_type":       "database",
# #             "external_database": ext_db,
# #             "new_user_db":       new_db,
# #             "tables":            []
# #         }

# #         ext_conn = ext_cur = None
# #         try:
# #             ext_conn = mysql.connector.connect(
# #                 host=MYSQL_CONFIG["host"], port=MYSQL_CONFIG["port"],
# #                 user=MYSQL_CONFIG["user"], password=MYSQL_CONFIG["password"],
# #                 database=new_db, connection_timeout=10
# #             )
# #             ext_cur = ext_conn.cursor(dictionary=True)
# #             ext_cur.execute("SHOW TABLES")
# #             tables = [list(r.values())[0] for r in ext_cur.fetchall()]

# #             for t in tables:
# #                 if not re.match(r'^\w+$', t):
# #                     continue
# #                 try:
# #                     ext_cur.execute(f"SELECT * FROM `{t}` LIMIT %s", (MAX_ROWS,))
# #                     rows = ext_cur.fetchall()
# #                     if not rows:
# #                         continue
# #                     cols = list(rows[0].keys())

# #                     # Compute column stats
# #                     col_stats = {}
# #                     for col in cols:
# #                         vals = [str(r[col]) for r in rows
# #                                 if r[col] is not None and str(r[col]).strip()]
# #                         distinct = list(dict.fromkeys(vals))
# #                         col_stats[col] = {
# #                             "total_values":    len(vals),
# #                             "distinct_values": len(distinct),
# #                             "sample":          distinct[:10]
# #                         }

# #                     db_result["tables"].append({
# #                         "table_name":  t,
# #                         "row_count":   len(rows),
# #                         "columns":     cols,
# #                         "column_stats": col_stats,
# #                         "sample_rows": [
# #                             {k: v for k, v in r.items()}
# #                             for r in rows[:5]
# #                         ]
# #                     })
# #                 except Exception as e:
# #                     print(f"[Analysis] table {t}: {e}")

# #         except Exception as e:
# #             print(f"[Analysis] connect {new_db}: {e}")
# #         finally:
# #             if ext_cur:  ext_cur.close()
# #             if ext_conn: ext_conn.close()

# #         results.append(db_result)

# #     return results


# # ══════════════════════════════════════════════════════
# # CONTEXT BUILDER
# # ══════════════════════════════════════════════════════

# def _build_context(web_data: list, db_data: list) -> str:
#     parts = []

#     for w in web_data:
#         lines = [f"=== WEB TOPIC: {w['topic']} ({w['result_count']} results) ==="]
#         for item in w["items"]:
#             lines.append(f"  Title : {item['title']}")
#             lines.append(f"  URL   : {item['url']}")
#             if item.get("brief"):
#                 lines.append(f"  Brief : {item['brief'][:300]}")
#         parts.append("\n".join(lines))

#     for d in db_data:
#         lines = [f"=== DATABASE: {d['external_database']} (stored as: {d['new_user_db']}) ==="]
#         for tbl in d["tables"]:
#             lines.append(f"\n  Table: {tbl['table_name']} ({tbl['row_count']} rows)")
#             lines.append(f"  Columns: {', '.join(tbl['columns'])}")
#             # Column stats
#             for col, stats in tbl["column_stats"].items():
#                 lines.append(
#                     f"    {col}: {stats['distinct_values']} distinct values — "
#                     f"sample: {', '.join(str(v) for v in stats['sample'][:8])}"
#                 )
#             # Sample rows
#             lines.append(f"  Sample rows (up to 5):")
#             for i, row in enumerate(tbl["sample_rows"], 1):
#                 r_str = " | ".join(f"{k}:{v}" for k,v in row.items()
#                                    if v is not None and str(v).strip())
#                 lines.append(f"    Row{i}: {r_str}")
#         parts.append("\n".join(lines))

#     ctx = "\n\n".join(parts)
#     if len(ctx) > MAX_CTX_CHARS:
#         ctx = ctx[:MAX_CTX_CHARS] + "\n\n[... truncated ...]"
#     return ctx


# # ══════════════════════════════════════════════════════
# # MISTRAL
# # ══════════════════════════════════════════════════════

# def _call_mistral(context: str, topics: list, databaseses: list) -> dict:
#     source_desc = []
#     if topics:    source_desc.append(f"web topics: {', '.join(topics)}")
#     if databaseses: source_desc.append(f"databaseses: {', '.join(databaseses)}")

#     system = """You are a senior data analyst who writes detailed textbook-style reports.
# Your report must be written as ONE continuous plain text — like a textbook chapter.
# Mix paragraphs and bullet points naturally. Minimum 15-20 lines of content.
# Use actual values, names, numbers from the data. Never be vague or generic.
# Respond ONLY in valid JSON with a single key: "report"."""

#     user = f"""
# Analyze the following data ({'; '.join(source_desc)}):

# {context}

# Write a comprehensive textbook-style analysis report. Return ONLY this JSON:
# {{
#   "report": "TITLE: <descriptive title here>\n\n<Opening paragraph — 3 to 4 sentences introducing what data was analyzed, how many sources, key highlights.>\n\n<Second paragraph — describe the main data sources, table names, row counts, column names found.>\n\n• <Bullet: specific fact with actual value from data>\n• <Bullet: another specific metric or count>\n• <Bullet: notable user/record/entry found>\n• <Bullet: pattern or trend observed>\n• <Bullet: another important data point>\n\n<Third paragraph — deeper analysis: relationships between tables, user activity, data patterns.>\n\n• <Bullet: cross-table insight>\n• <Bullet: most active user or top record>\n• <Bullet: date range or time pattern>\n• <Bullet: data distribution observation>\n• <Bullet: anomaly or interesting finding>\n\n<Fourth paragraph — data quality and completeness observations.>\n\n• <Bullet: data quality note>\n• <Bullet: missing or null value observation>\n\n<Fifth paragraph — recommendations and conclusions based on the data.>\n\n• <Bullet: actionable recommendation>\n• <Bullet: another recommendation>\n• <Bullet: conclusion>"
# }}

# RULES:
# - Replace all <...> placeholders with REAL content from the data above.
# - Minimum 18 lines inside the report string.
# - Use \n for newlines inside the JSON string.
# - Every bullet point must have a specific value/name/number from the actual data.
# - Do NOT use generic filler — every sentence must reference actual data.
# """
#     headers = {"Authorization": f"Bearer {MISTRAL_API_KEY}",
#                "Content-Type": "application/json", "Accept": "application/json"}
#     payload = {
#         "model": MISTRAL_MODEL,
#         "messages": [{"role":"system","content":system},
#                      {"role":"user","content":user}],
#         "response_format": {"type":"json_object"},
#         "temperature": 0.2
#     }
#     try:
#         resp = requests.post(MISTRAL_URL, headers=headers, json=payload, timeout=120)
#         resp.raise_for_status()
#         return json.loads(resp.json()["choices"][0]["message"]["content"])
#     except Exception as e:
#         print(f"[Mistral] {e}")
#         return None


# # ══════════════════════════════════════════════════════
# # MAIN CONTROLLER  —  POST /session-analysis
# # ══════════════════════════════════════════════════════

# def session_analysis_controller(get_connection_func):
#     data       = request.json or {}
#     session_id = (data.get("session_id") or "").strip()
#     topics     = [t.strip() for t in (data.get("topics")    or []) if str(t).strip()]
#     databaseses  = [d.strip() for d in (data.get("databaseses") or []) if str(d).strip()]

#     if not session_id:
#         return jsonify({
#             "status":"failed","statusCode":400,
#             "message":"Field 'session_id' is required."
#         }), 400

#     if not topics and not databaseses:
#         return jsonify({
#             "status":"failed","statusCode":400,
#             "message":"At least one of 'topics' or 'databaseses' must be provided."
#         }), 400

#     conn = None
#     try:
#         conn = get_connection_func()

#         web_data = _fetch_web_data(session_id, topics, conn)
#         db_data  = _fetch_db_data(session_id, databaseses, conn)

#         if not web_data and not db_data:
#             return jsonify({
#                 "status":"no_data","statusCode":200,
#                 "session_id":session_id,
#                 "message":"No matching data found for the provided topics/databaseses."
#             }), 200

#         # Build raw data summary (always returned)
#         raw_summary = {
#             "web_sources":      web_data,
#             "database_sources": [
#                 {
#                     "external_database": d["external_database"],
#                     "new_user_db":       d["new_user_db"],
#                     "tables": [
#                         {
#                             "table_name":  t["table_name"],
#                             "row_count":   t["row_count"],
#                             "columns":     t["columns"],
#                             "sample_rows": t["sample_rows"]
#                         }
#                         for t in d["tables"]
#                     ]
#                 }
#                 for d in db_data
#             ]
#         }

#         # Build LLM context
#         context = _build_context(web_data, db_data)

#         # Call Mistral for analysis
#         analysis = _call_mistral(context, topics, databaseses)
#         # Generate Graph
#         graph_url = generate_session_graph(session_id, web_data, db_data)

#         if not analysis:
#             return jsonify({
#                 "status":"partial","statusCode":200,
#                 "session_id":session_id,
#                 "message":"LLM analysis failed, returning raw data only.",
#                 "raw_data": raw_summary
#             }), 200

#         return jsonify({
#             "status":     "success",
#             "statusCode": 200,
#             "session_id": session_id,
#             "requested": {
#                 "topics":    topics,
#                 "databaseses": databaseses
#             },
#             "report": analysis.get("report", ""),
#             "graph_url": graph_url,
#             "raw_data":  raw_summary
#         }), 200

#     except Exception as e:
#         return jsonify({
#             "status":"error","statusCode":500,
#             "message":str(e)
#         }), 500
#     finally:
#         if conn: conn.close()


# controllers/session_analysis_controller.py
#
# POST /session-analysis
#
# Body:
# {
#   "session_id": "xxx",
#   "topics":     ["AI", "Python"],           // optional
#   "databases":  ["recipe", "O2C"]           // optional
# }
#
# Cache logic:
#   - Builds a SHA-256 hash of the raw context (DB rows + web briefs).
#   - If session_id exists in session_analysis_cache AND hash matches
#     → return saved report + graph_url immediately (no LLM call).
#   - If session_id is new OR hash differs (data changed)
#     → call LLM, generate graph, upsert cache row.

import hashlib
import re
import json
import requests
import mysql.connector
from flask import request, jsonify
from database.config import MISTRAL_API_KEY, MISTRAL_MODEL, MYSQL_CONFIG

from pyvis.network import Network
import os
import uuid
from database.config import GRAPH_FOLDER, BASE_URL

MISTRAL_URL   = "https://api.mistral.ai/v1/chat/completions"
MAX_ROWS      = 100
MAX_CTX_CHARS = 24000


# ══════════════════════════════════════════════════════
# CACHE HELPERS
# ══════════════════════════════════════════════════════

def _hash_context(context: str) -> str:
    """Return SHA-256 hex digest of the raw context string."""
    return hashlib.sha256(context.encode("utf-8")).hexdigest()


def _load_cache(session_id: str, data_hash: str, conn) -> dict | None:
    """
    Return cached {report, graph_url} if session_id exists AND hash matches.
    Returns None otherwise (miss or stale).
    """
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT `report`, `graph_url`, `data_hash`
            FROM `session_analysis_cache`
            WHERE `session_id` = %s
            LIMIT 1
        """, (session_id,))
        row = cursor.fetchone()
        if row and row["data_hash"] == data_hash:
            print(f"[Cache] HIT   session={session_id}")
            return {"report": row["report"], "graph_url": row["graph_url"]}
        if row:
            print(f"[Cache] STALE session={session_id} — data changed, regenerating")
        else:
            print(f"[Cache] MISS  session={session_id} — first time")
        return None
    except Exception as e:
        print(f"[Cache] load error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()


def _save_cache(session_id: str, data_hash: str,
                report: str, graph_url: str,
                topics: list, databases: list, conn) -> None:
    """Upsert cache row for this session."""
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO `session_analysis_cache`
                (`session_id`, `data_hash`, `report`, `graph_url`, `topics`, `databases`)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `data_hash`  = VALUES(`data_hash`),
                `report`     = VALUES(`report`),
                `graph_url`  = VALUES(`graph_url`),
                `topics`     = VALUES(`topics`),
                `databases`  = VALUES(`databases`),
                `updated_at` = CURRENT_TIMESTAMP
        """, (
            session_id,
            data_hash,
            report,
            graph_url,
            json.dumps(topics),
            json.dumps(databases)
        ))
        conn.commit()
        print(f"[Cache] SAVED session={session_id}  topics={topics}  databases={databases}")
    except Exception as e:
        print(f"[Cache] save error: {e}")
        raise   # re-raise so the caller knows save failed
    finally:
        if cursor:
            cursor.close()


# ══════════════════════════════════════════════════════
# CROSS-SOURCE RELATIONSHIP DETECTION
# ══════════════════════════════════════════════════════

def detect_cross_source_relationships(table_columns: dict, web_data: list, db_data: list) -> dict:
    """
    Ask Mistral to find ALL relationships:
      1. DB table  <-> DB table   (column-level)
      2. DB column <-> Web topic/item (value / keyword overlap)

    Returns:
    {
      "db_db":  [ {table1, column1, table2, column2, reason}, ... ],
      "db_web": [ {table, column, topic, item_title, reason}, ... ]
    }
    """

    db_summary = {}
    for db in db_data:
        for tbl in db["tables"]:
            entry = {}
            for col, stats in tbl.get("column_stats", {}).items():
                entry[col] = stats.get("sample", [])[:6]
            db_summary[tbl["table_name"]] = entry

    web_summary = []
    for w in web_data:
        for item in w["items"]:
            web_summary.append({
                "topic": w["topic"],
                "title": item["title"],
                "brief": (item.get("brief") or "")[:200]
            })

    prompt = f"""
You are a senior data analyst.

## DB Tables with columns and sample values:
{json.dumps(db_summary, indent=2)}

## Web search results (topic / title / brief):
{json.dumps(web_summary, indent=2)}

## Task
Find ALL meaningful relationships across these two categories:

1. **DB <-> DB**: columns in different tables that likely represent the same entity
   (matching names, matching sample values, or clear FK/PK patterns).

2. **DB <-> Web**: a DB table column whose sample values (or column name) clearly
   appear in or are directly related to a web result's topic, title, or brief.

Return ONLY valid JSON - no markdown, no extra text:
{{
  "db_db": [
    {{
      "table1": "table_name",
      "column1": "col_name",
      "table2": "table_name",
      "column2": "col_name",
      "reason": "short reason"
    }}
  ],
  "db_web": [
    {{
      "table": "table_name",
      "column": "col_name",
      "topic": "web topic",
      "item_title": "web result title",
      "reason": "short reason"
    }}
  ]
}}

Rules:
- Only include relationships genuinely supported by the data.
- If no relationships exist for a category, return an empty list for that key.
- Keep reasons under 15 words.
"""

    try:
        resp = requests.post(
            MISTRAL_URL,
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": MISTRAL_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "temperature": 0
            },
            timeout=90
        )
        raw = resp.json()["choices"][0]["message"]["content"]
        result = json.loads(raw)
        result.setdefault("db_db", [])
        result.setdefault("db_web", [])
        return result

    except Exception as e:
        print("Cross-source relationship detection error:", e)
        return {"db_db": [], "db_web": []}


# ══════════════════════════════════════════════════════
# GRAPH GENERATOR
# ══════════════════════════════════════════════════════

def generate_session_graph(session_id, web_data, db_data):

    net = Network(
        height="850px",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        directed=False
    )

    SESSION_STYLE  = {"color": "#00bfa5", "size": 35}
    TOPIC_STYLE    = {"color": "#ff4081", "size": 20}
    WEB_ITEM_STYLE = {"color": "#f06292", "size": 12}
    DB_STYLE       = {"color": "#2979ff", "size": 25}
    TABLE_STYLE    = {"color": "#ffc107", "size": 15}
    COL_STYLE      = {"color": "#9ccc65", "size": 10}

    session_node = f"session_{session_id}"
    net.add_node(session_node, label=f"Session\n{session_id[:8]}...", **SESSION_STYLE)

    # Web Topics + Items
    for w in web_data:
        topic      = w["topic"]
        topic_node = f"topic_{topic}"
        net.add_node(topic_node, label=topic, **TOPIC_STYLE)
        net.add_edge(session_node, topic_node)

        for item in w["items"]:
            title     = item["title"][:45]
            item_node = f"webitem_{topic}_{title[:20]}"
            net.add_node(item_node, label=title, **WEB_ITEM_STYLE)
            net.add_edge(topic_node, item_node)

    # DB nodes
    table_columns = {}
    for db in db_data:
        db_name = db["external_database"]
        db_node = f"db_{db_name}"
        net.add_node(db_node, label=db_name, **DB_STYLE)
        net.add_edge(session_node, db_node)

        for table in db["tables"]:
            tname   = table["table_name"]
            columns = table.get("columns", [])
            table_columns[tname] = columns

            tnode = f"table_{tname}"
            net.add_node(tnode, label=tname, shape="box", **TABLE_STYLE)
            net.add_edge(db_node, tnode)

            for col in columns:
                col_node = f"col_{tname}.{col}"
                net.add_node(col_node, label=col, **COL_STYLE)
                net.add_edge(tnode, col_node)

    # Cross-source relationships
    relationships = detect_cross_source_relationships(table_columns, web_data, db_data)

    # DB <-> DB  (red solid edges)
    for r in relationships.get("db_db", []):
        try:
            src    = f"col_{r['table1']}.{r['column1']}"
            dst    = f"col_{r['table2']}.{r['column2']}"
            reason = r.get("reason", "related")
            net.add_edge(src, dst,
                         color="#ff5252", width=4,
                         label=f"[REL] {reason}", title=reason)
            print(f"[Graph] DB<->DB  {src} -> {dst} ({reason})")
        except Exception as e:
            print("DB<->DB edge error:", e)

    # DB <-> Web  (orange dashed edges)
    existing_ids = {n["id"] for n in net.nodes}
    for r in relationships.get("db_web", []):
        try:
            col_node   = f"col_{r['table']}.{r['column']}"
            topic      = r.get("topic", "")
            item_title = r.get("item_title", "")
            candidate  = f"webitem_{topic}_{item_title[:20]}"
            web_node   = candidate if candidate in existing_ids else f"topic_{topic}"
            reason     = r.get("reason", "related")

            if col_node in existing_ids and web_node in existing_ids:
                net.add_edge(col_node, web_node,
                             color="#ff9800", width=3, dashes=True,
                             label=f"[WEB] {reason}", title=reason)
                print(f"[Graph] DB<->Web {col_node} -> {web_node} ({reason})")
            else:
                print(f"[Graph] Skipped DB<->Web — node missing: {col_node} / {web_node}")
        except Exception as e:
            print("DB<->Web edge error:", e)

    html_filename = f"graph_{uuid.uuid4().hex[:8]}.html"
    html_path     = os.path.join(GRAPH_FOLDER, html_filename)
    net.save_graph(html_path)

    return f"{BASE_URL}/graphs/{html_filename}"


# ══════════════════════════════════════════════════════
# DATA FETCHERS
# ══════════════════════════════════════════════════════

def _fetch_web_data(session_id: str, topics: list, conn) -> list:
    if not topics:
        return []
    cursor = None
    results = []
    try:
        cursor = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(topics))
        cursor.execute(f"""
            SELECT topic, title, url, brief, saved_at
            FROM saved_web_results
            WHERE `session_id` = %s
              AND topic IN ({placeholders})
            ORDER BY topic, saved_at DESC
        """, [session_id] + topics)
        rows = cursor.fetchall()

        grouped = {}
        for r in rows:
            t = r["topic"]
            grouped.setdefault(t, []).append({
                "title":    r["title"],
                "url":      r["url"],
                "brief":    r.get("brief", ""),
                "saved_at": str(r["saved_at"]) if r["saved_at"] else None
            })

        for topic, items in grouped.items():
            results.append({
                "source_type":  "web",
                "topic":        topic,
                "result_count": len(items),
                "items":        items
            })
    except Exception as e:
        print(f"[Analysis] web fetch error: {e}")
    finally:
        if cursor: cursor.close()
    return results


def _fetch_db_data(session_id: str, databases: list, conn) -> list:
    if not databases:
        return []

    cursor = None
    results = []

    try:
        cursor = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(databases))
        cursor.execute(f"""
            SELECT DISTINCT external_database, new_user_db
            FROM external_db_sync_log
            WHERE `session_id` = %s
              AND external_database IN ({placeholders})
              AND new_user_db IS NOT NULL
              AND new_user_db != ''
        """, [session_id] + databases)
        rows   = cursor.fetchall()
        db_map = {r["external_database"]: r["new_user_db"] for r in rows}
        print(f"[Analysis] DB MAP -> {db_map}")
    except Exception as e:
        print(f"[Analysis] db_map error: {e}")
        return []
    finally:
        if cursor: cursor.close()

    for ext_db in databases:
        new_db = db_map.get(ext_db)
        if not new_db:
            print(f"[Analysis] No mapped DB for {ext_db}")
            continue

        db_result = {
            "source_type":       "database",
            "external_database": ext_db,
            "new_user_db":       new_db,
            "tables":            []
        }

        ext_conn = ext_cur = None
        try:
            print(f"[Analysis] Connecting DB -> {new_db}")
            ext_conn = mysql.connector.connect(
                host=MYSQL_CONFIG["host"],
                port=MYSQL_CONFIG["port"],
                user=MYSQL_CONFIG["user"],
                password=MYSQL_CONFIG["password"],
                database=new_db,
                connection_timeout=10
            )
            ext_cur = ext_conn.cursor(dictionary=True)

            ext_cur.execute("SHOW TABLES")
            tables = [list(r.values())[0] for r in ext_cur.fetchall()]
            print(f"[Analysis] Tables in {new_db} -> {tables}")

            for t in tables:
                print(f"[Analysis] Reading table -> {t}")
                try:
                    ext_cur.execute(f"SELECT * FROM `{t}` LIMIT %s", (MAX_ROWS,))
                    rows      = ext_cur.fetchall()
                    row_count = len(rows)
                    cols      = list(rows[0].keys()) if row_count > 0 else []

                    col_stats = {}
                    if row_count > 0:
                        for col in cols:
                            vals = [
                                str(r[col]).strip()
                                for r in rows
                                if r.get(col) is not None and str(r[col]).strip()
                            ]
                            distinct_vals = list(dict.fromkeys(vals))
                            col_stats[col] = {
                                "total_values":    len(vals),
                                "distinct_values": len(distinct_vals),
                                "sample":          distinct_vals[:10]
                            }

                    db_result["tables"].append({
                        "table_name":   t,
                        "row_count":    row_count,
                        "columns":      cols,
                        "column_stats": col_stats,
                        "sample_rows":  rows[:5] if rows else []
                    })
                except Exception as table_error:
                    print(f"[Analysis] table {t} error -> {table_error}")

        except Exception as db_error:
            print(f"[Analysis] connect {new_db} error -> {db_error}")
        finally:
            if ext_cur:
                try: ext_cur.close()
                except: pass
            if ext_conn:
                try: ext_conn.close()
                except: pass

        results.append(db_result)

    return results


# ══════════════════════════════════════════════════════
# CONTEXT BUILDER
# ══════════════════════════════════════════════════════

def _build_context(web_data: list, db_data: list) -> str:
    parts = []

    for w in web_data:
        lines = [f"=== WEB TOPIC: {w['topic']} ({w['result_count']} results) ==="]
        for item in w["items"]:
            lines.append(f"  Title : {item['title']}")
            lines.append(f"  URL   : {item['url']}")
            if item.get("brief"):
                lines.append(f"  Brief : {item['brief'][:300]}")
        parts.append("\n".join(lines))

    for d in db_data:
        lines = [f"=== DATABASE: {d['external_database']} (stored as: {d['new_user_db']}) ==="]
        for tbl in d["tables"]:
            lines.append(f"\n  Table: {tbl['table_name']} ({tbl['row_count']} rows)")
            lines.append(f"  Columns: {', '.join(tbl['columns'])}")
            for col, stats in tbl["column_stats"].items():
                lines.append(
                    f"    {col}: {stats['distinct_values']} distinct -- "
                    f"sample: {', '.join(str(v) for v in stats['sample'][:8])}"
                )
            lines.append("  Sample rows (up to 5):")
            for i, row in enumerate(tbl["sample_rows"], 1):
                r_str = " | ".join(
                    f"{k}:{v}" for k, v in row.items()
                    if v is not None and str(v).strip()
                )
                lines.append(f"    Row{i}: {r_str}")
        parts.append("\n".join(lines))

    ctx = "\n\n".join(parts)
    if len(ctx) > MAX_CTX_CHARS:
        ctx = ctx[:MAX_CTX_CHARS] + "\n\n[... truncated ...]"
    return ctx


# ══════════════════════════════════════════════════════
# MISTRAL — REPORT GENERATION
# ══════════════════════════════════════════════════════

def _call_mistral(context: str, topics: list, databases: list) -> dict:
    source_desc = []
    if topics:    source_desc.append(f"web topics: {', '.join(topics)}")
    if databases: source_desc.append(f"databases: {', '.join(databases)}")

    system = """You are a senior data analyst who writes detailed textbook-style reports.
Your report must be written as ONE continuous plain text like a textbook chapter.
Mix paragraphs and bullet points naturally. Minimum 15-20 lines of content.
Use actual values, names, numbers from the data. Never be vague or generic.
Respond ONLY in valid JSON with a single key: "report"."""

    user = f"""
Analyze the following data ({'; '.join(source_desc)}):

{context}

Write a comprehensive textbook-style analysis report. Return ONLY this JSON:
{{
  "report": "TITLE: <descriptive title here>\\n\\n<Opening paragraph 3-4 sentences>\\n\\n<Second paragraph: data sources, table names, row counts, columns>\\n\\n- <bullet: specific fact with actual value>\\n- <bullet: metric or count>\\n- <bullet: notable record>\\n- <bullet: pattern or trend>\\n\\n<Third paragraph: deeper analysis, relationships, patterns>\\n\\n- <bullet: cross-source insight>\\n- <bullet: top record>\\n- <bullet: date/time pattern>\\n- <bullet: anomaly>\\n\\n<Fourth paragraph: data quality observations>\\n\\n- <bullet: quality note>\\n- <bullet: null values>\\n\\n<Fifth paragraph: recommendations and conclusions>\\n\\n- <bullet: recommendation>\\n- <bullet: conclusion>"
}}

RULES:
- Replace all <...> with REAL content from the actual data.
- Minimum 18 lines inside the report string.
- Use \\n for newlines inside the JSON string.
- Every bullet must reference a specific value, name, or number from the data.
- Do NOT use generic filler sentences.
"""
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json"
    }
    payload = {
        "model": MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.2
    }
    try:
        resp = requests.post(MISTRAL_URL, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        return json.loads(resp.json()["choices"][0]["message"]["content"])
    except Exception as e:
        print(f"[Mistral] {e}")
        return None


# ══════════════════════════════════════════════════════
# MAIN CONTROLLER  —  POST /session-analysis
# ══════════════════════════════════════════════════════

def session_analysis_controller(get_connection_func):
    data       = request.json or {}
    session_id = (data.get("session_id") or "").strip()
    topics     = [t.strip() for t in (data.get("topics")    or []) if str(t).strip()]
    databases  = [d.strip() for d in (data.get("databases") or []) if str(d).strip()]

    if not session_id:
        return jsonify({
            "status": "failed", "statusCode": 400,
            "message": "Field 'session_id' is required."
        }), 400

    if not topics and not databases:
        return jsonify({
            "status": "failed", "statusCode": 400,
            "message": "At least one of 'topics' or 'databases' must be provided."
        }), 400

    conn = None
    try:
        conn = get_connection_func()

        # 1. Fetch raw data
        web_data = _fetch_web_data(session_id, topics, conn)
        db_data  = _fetch_db_data(session_id, databases, conn)

        if not web_data and not db_data:
            return jsonify({
                "status":     "no_data",
                "statusCode": 200,
                "message":    "No matching data found for the provided topics/databases."
            }), 200

        # 2. Build context + hash
        context   = _build_context(web_data, db_data)
        data_hash = _hash_context(context)

        # 3. Raw summary (always returned in response)
        raw_summary = {
            "web_sources": web_data,
            "database_sources": [
                {
                    "external_database": d["external_database"],
                    "new_user_db":       d["new_user_db"],
                    "tables": [
                        {
                            "table_name":  t["table_name"],
                            "row_count":   t["row_count"],
                            "columns":     t["columns"],
                            "sample_rows": t["sample_rows"]
                        }
                        for t in d["tables"]
                    ]
                }
                for d in db_data
            ]
        }

        # 4. Check cache
        cached = _load_cache(session_id, data_hash, conn)

        if cached:
            # Cache HIT — return immediately, no LLM call
            return jsonify({
                "status":     "success",
                "statusCode": 200,
                "report":     cached["report"],
                "graph_url":  cached["graph_url"],
            }), 200

        # 5. Cache MISS or STALE — generate fresh
        analysis  = _call_mistral(context, topics, databases)
        graph_url = generate_session_graph(session_id, web_data, db_data)

        if not analysis:
            return jsonify({
                "status":     "partial",
                "statusCode": 200,
                "message":    "LLM analysis failed.",
            }), 200

        report = analysis.get("report", "")

        # 6. Save to cache
        _save_cache(session_id, data_hash, report, graph_url, topics, databases, conn)

        return jsonify({
            "status":     "success",
            "statusCode": 200,
            "report":     report,
            "graph_url":  graph_url,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error", "statusCode": 500,
            "message": str(e)
        }), 500
    finally:
        if conn: conn.close()