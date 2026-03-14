# controllers/session_chat_history_controller.py
#
# Endpoints:
#   POST   /session-chat-history/save    — save one Q&A turn
#   GET    /session-chat-history?session_id=xxx&user_id=xxx  — get full history
#   DELETE /session-chat-history?session_id=xxx&user_id=xxx  — clear history

import json
from flask import request, jsonify


# ── SQL to create the table (run once) ──────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS session_chat_history (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    session_id    VARCHAR(100) NOT NULL,
    user_id       INT          NOT NULL,
    turn_index    INT          NOT NULL DEFAULT 0,
    question      TEXT         NOT NULL,
    answer        LONGTEXT     NOT NULL,
    follow_up_questions JSON   DEFAULT NULL,
    intent        VARCHAR(50)  DEFAULT NULL,
    mode          VARCHAR(30)  DEFAULT 'answer',
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_user    (user_id),
    INDEX idx_session_user (session_id, user_id)
);
"""


def _ensure_table(cursor):
    cursor.execute(CREATE_TABLE_SQL)


# ══════════════════════════════════════════════════════
# POST /session-chat-history/save
# Body: { session_id, user_id, question, answer,
#         follow_up_questions?, intent?, mode? }
# ══════════════════════════════════════════════════════
def save_chat_history(get_connection_func):
    data       = request.json or {}
    session_id = (data.get("session_id") or "").strip()
    user_id    = data.get("user_id")
    question   = (data.get("question")   or "").strip()
    answer     = (data.get("answer")     or "").strip()

    if not session_id:
        return jsonify({"status":"failed","statusCode":400,
                        "message":"session_id is required"}), 400
    if not user_id:
        return jsonify({"status":"failed","statusCode":400,
                        "message":"user_id is required"}), 400
    if not question or not answer:
        return jsonify({"status":"failed","statusCode":400,
                        "message":"question and answer are required"}), 400

    follow_ups = data.get("follow_up_questions") or []
    intent     = (data.get("intent") or "").strip() or None
    mode       = (data.get("mode")   or "answer").strip()

    conn = cursor = None
    try:
        conn   = get_connection_func()
        cursor = conn.cursor(dictionary=True)
        _ensure_table(cursor)

        # Get current turn count for this session+user
        cursor.execute("""
            SELECT COALESCE(MAX(turn_index), -1) AS last_turn
            FROM session_chat_history
            WHERE session_id = %s AND user_id = %s
        """, (session_id, int(user_id)))
        row        = cursor.fetchone()
        turn_index = (row["last_turn"] + 1) if row else 0

        cursor.execute("""
            INSERT INTO session_chat_history
                (session_id, user_id, turn_index, question, answer,
                 follow_up_questions, intent, mode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            session_id,
            int(user_id),
            turn_index,
            question,
            answer,
            json.dumps(follow_ups) if follow_ups else None,
            intent,
            mode
        ))
        conn.commit()
        new_id = cursor.lastrowid

        return jsonify({
            "status":     "success",
            "statusCode": 201,
            "id":         new_id,
            "turn_index": turn_index,
            "message":    "Chat turn saved."
        }), 201

    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        return jsonify({"status":"error","statusCode":500,"message":str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ══════════════════════════════════════════════════════
# GET /session-chat-history?session_id=xxx&user_id=xxx
# ══════════════════════════════════════════════════════
# def get_chat_history(get_connection_func):
#     from collections import defaultdict
#     import json
#     from flask import request, jsonify
    
#     session_id = request.args.get('session_id')
#     user_id = request.args.get('user_id')
#     workspace_name = request.args.get('workspace_name', 'My Workspace')

#     if not session_id or not user_id:
#         return jsonify({"status": "error", "statusCode": 400, "message": "Missing IDs"}), 400

#     conn = cur = None
#     try:
#         conn = get_connection_func()
#         cur = conn.cursor(dictionary=True)

#         # 1. ADD visualizations and follow_up_questions TO THE SQL SELECT
#         cur.execute("""
#             SELECT id, visit_number, question, answer, follow_up_questions, visualizations
#             FROM session_chat_history
#             WHERE session_id = %s AND user_id = %s
#             ORDER BY turn_index ASC
#         """, (session_id, int(user_id)))
        
#         rows = cur.fetchall()
#         grouped_sessions = defaultdict(list)

#         for row in rows:
#             visit_num = row.get("visit_number", 1)
            
#             # 2. SAFELY PARSE THE JSON STRINGS BACK INTO ARRAYS
#             fuq = []
#             if row.get("follow_up_questions"):
#                 try: fuq = json.loads(row["follow_up_questions"])
#                 except: pass
                
#             viz = []
#             if row.get("visualizations"):
#                 try: viz = json.loads(row["visualizations"])
#                 except: pass

#             # 3. ADD THEM TO YOUR QUESTION OBJECT
#             grouped_sessions[visit_num].append({
#                 "questionId": f"{row['id']}",
#                 "question": row["question"],
#                 "answer": row["answer"],
#                 "follow_up_questions": fuq,    # <--- NOW INCLUDED
#                 "visualizations": viz          # <--- NOW INCLUDED
#             })

#         workspaceQueryHistory = []
#         for v_num in sorted(grouped_sessions.keys()):
#             workspaceQueryHistory.append({
#                 "querySessionId": f"session_visit_{v_num}", 
#                 "querySessionHistory": grouped_sessions[v_num]
#             })

#         final_response = {
#             "querySessions": [
#                 {
#                     "querySessionName": workspace_name,
#                     "querrySessionId": session_id,
#                     "workspaceQueryHistory": workspaceQueryHistory
#                 }
#             ],
#             # "workspaceFolderId": session_id,
#             "status": "success",
#             "statusCode": 200,
#             # "total_turns": len(rows),
#             # "user_id": str(user_id)
#         }

#         return jsonify(final_response), 200

#     except Exception as e:
#         return jsonify({"status": "error", "statusCode": 500, "message": str(e)}), 500
#     finally:
#         if cur: cur.close()
#         if conn: conn.close()
def get_chat_history(get_connection_func):
    from collections import defaultdict
    import json
    from flask import request, jsonify
    
    session_id = request.args.get('session_id')
    user_id = request.args.get('user_id')
    workspace_name = request.args.get('workspace_name', 'chatSessionName')

    if not session_id or not user_id:
        return jsonify({"status": "error", "statusCode": 400, "message": "Missing IDs"}), 400

    conn = cur = None
    try:
        conn = get_connection_func()
        cur = conn.cursor(dictionary=True)

        # 1. Fetch data from the database
        cur.execute("""
            SELECT id, visit_number, question, answer, follow_up_questions, visualizations
            FROM session_chat_history
            WHERE session_id = %s AND user_id = %s
            ORDER BY turn_index ASC
        """, (session_id, int(user_id)))
        
        rows = cur.fetchall()
        grouped_sessions = defaultdict(list)

        # 2. Group the chats and safely parse JSON
        for row in rows:
            visit_num = row.get("visit_number", 1)
            
            fuq = []
            if row.get("follow_up_questions"):
                try: fuq = json.loads(row["follow_up_questions"])
                except: pass
                
            viz = []
            if row.get("visualizations"):
                try: viz = json.loads(row["visualizations"])
                except: pass

            grouped_sessions[visit_num].append({
                "questionId": str(row['id']),
                "question": row["question"],
                "answer": row["answer"],
                "follow_up_questions": fuq,    
                "visualizations": viz          
            })

        # 3. Build the querySessions array exactly as requested
        querySessions = []
        for v_num in sorted(grouped_sessions.keys()):
            querySessions.append({
                # "sessionId": session_id,                        # e.g., "f9c29d15..."
                "querySessionName": workspace_name,             # e.g., "My Workspace"
                "querySessionId": f"session_visit_{v_num}",     # e.g., "session_visit_1"
                "querySessionHistory": grouped_sessions[v_num]  # The list of chats
            })

        # 4. Build the final response
        final_response = {
            "querySessions": querySessions,
            "status": "success",
            "statusCode": 200
        }

        return jsonify(final_response), 200

    except Exception as e:
        return jsonify({"status": "error", "statusCode": 500, "message": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()



# ══════════════════════════════════════════════════════
# DELETE /session-chat-history?session_id=xxx&user_id=xxx
# ══════════════════════════════════════════════════════
def delete_chat_history(get_connection_func):
    session_id = (request.args.get("session_id") or "").strip()
    user_id    = request.args.get("user_id")

    if not session_id or not user_id:
        return jsonify({"status":"failed","statusCode":400,
                        "message":"session_id and user_id are required"}), 400

    conn = cursor = None
    try:
        conn   = get_connection_func()
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM session_chat_history
            WHERE session_id = %s AND user_id = %s
        """, (session_id, int(user_id)))
        conn.commit()
        deleted = cursor.rowcount

        return jsonify({
            "status":        "success",
            "statusCode":    200,
            "deleted_turns": deleted,
            "message":       f"Deleted {deleted} chat turn(s)."
        }), 200

    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        return jsonify({"status":"error","statusCode":500,"message":str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ══════════════════════════════════════════════════════
# MAIN DISPATCHER
# ══════════════════════════════════════════════════════
def session_chat_history_controller(get_connection_func):
    if request.method == "POST":
        return save_chat_history(get_connection_func)
    elif request.method == "GET":
        return get_chat_history(get_connection_func)
    elif request.method == "DELETE":
        return delete_chat_history(get_connection_func)
    return jsonify({"status":"failed","statusCode":405,"message":"Method not allowed"}), 405