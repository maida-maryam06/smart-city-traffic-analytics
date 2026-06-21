"""
Smart City Traffic Analytics - Flask Backend
All bugs fixed:
  - Thread-safe MySQL via per-request connections (no global conn)
  - MongoDB lazy reconnect via get_db() wrapper
  - Startup retry loop waits for Docker services to be ready
  - serialize_doc() no longer mutates original dicts
  - Proper teardown / connection cleanup
  - Input validation on all endpoints
  - random imported at top level
"""

import os
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from functools import wraps

import bcrypt
import pymysql
import pymysql.cursors
from bson.objectid import ObjectId
from flask import Flask, jsonify, request, send_from_directory, g
from flask_cors import CORS
from pymongo import MongoClient, errors as mongo_errors
from pymongo.errors import ServerSelectionTimeoutError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("traffic")

# ---------------------------------------------------------------------------
# Config (all from environment so Docker env-vars work seamlessly)
# ---------------------------------------------------------------------------
MONGO_URI        = os.environ.get("MONGO_URI",        "mongodb://localhost:27017/")
MYSQL_HOST       = os.environ.get("MYSQL_HOST",       "localhost")
MYSQL_PORT       = int(os.environ.get("MYSQL_PORT",   "3306"))
MYSQL_USER       = os.environ.get("MYSQL_USER",       "root")
MYSQL_PASSWORD   = os.environ.get("MYSQL_PASSWORD",   "rootpassword")
MYSQL_DATABASE   = os.environ.get("MYSQL_DATABASE",   "traffic_db")
ADMIN_SECRET     = os.environ.get("ADMIN_SECRET",     "ADMIN123")

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ---------------------------------------------------------------------------
# MongoDB  —  single MongoClient (thread-safe by design), lazy reconnect
# ---------------------------------------------------------------------------
_mongo_client: MongoClient | None = None

def get_mongo_client() -> MongoClient | None:
    """Return a live MongoClient, reconnecting silently if needed."""
    global _mongo_client
    try:
        if _mongo_client is None:
            _mongo_client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=3000,
                connectTimeoutMS=3000,
                socketTimeoutMS=5000,
                retryWrites=True,
                retryReads=True,
            )
        # Cheap ping to verify the connection is still alive
        _mongo_client.admin.command("ping")
        return _mongo_client
    except Exception as exc:
        log.warning("MongoDB unreachable (%s) – reconnecting next call", exc)
        _mongo_client = None
        return None

def get_db():
    """Return the traffic_analytics database or None."""
    client = get_mongo_client()
    return client["traffic_analytics"] if client else None

# ---------------------------------------------------------------------------
# MySQL  —  one fresh connection per Flask request (thread-safe, no pool needed
#            for typical single-container use; swap for a pool if you scale out)
# ---------------------------------------------------------------------------
def open_mysql() -> pymysql.connections.Connection | None:
    """Open a fresh MySQL connection. Returns None on failure."""
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )
        return conn
    except Exception as exc:
        log.warning("MySQL connection failed: %s", exc)
        return None

def get_mysql() -> pymysql.connections.Connection | None:
    """
    Return the MySQL connection for this request, opening it lazily.
    Stored on Flask's `g` object so it is closed automatically after
    the request via the teardown hook below.
    """
    if "mysql" not in g:
        g.mysql = open_mysql()
    elif not g.mysql.open:
        g.mysql = open_mysql()
    return g.mysql

@app.teardown_appcontext
def close_mysql(exc=None):
    """Close the per-request MySQL connection."""
    conn = g.pop("mysql", None)
    if conn is not None and conn.open:
        try:
            conn.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def serialize_doc(doc: dict) -> dict:
    """Return a *copy* of a MongoDB doc with ObjectId converted to str."""
    if doc is None:
        return {}
    out = dict(doc)
    if "_id" in out:
        out["_id"] = str(out["_id"])
    # Remove non-JSON-serialisable datetime stored for querying
    if "created_at" in out and isinstance(out["created_at"], datetime):
        out.pop("created_at")
    return out

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def require_json(f):
    """Decorator: return 400 if request body is not valid JSON."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400
        return f(*args, **kwargs)
    return wrapper

# ---------------------------------------------------------------------------
# Startup: wait for both databases to be ready before serving
# ---------------------------------------------------------------------------
def _ensure_mongo_indexes(db):
    db.traffic_data.create_index("created_at")
    db.traffic_data.create_index("timestamp")
    db.traffic_data.create_index([("road_name", 1), ("created_at", -1)])
    db.congestion_alerts.create_index([("resolved", 1), ("timestamp", -1)])
    log.info("MongoDB indexes ensured.")

def _ensure_mysql_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            INT AUTO_INCREMENT PRIMARY KEY,
                username      VARCHAR(50)  UNIQUE NOT NULL,
                email         VARCHAR(100) UNIQUE NOT NULL,
                password_hash VARCHAR(255)        NOT NULL,
                is_admin      BOOLEAN      DEFAULT FALSE,
                created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                updated_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_username (username),
                INDEX idx_email    (email)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
    log.info("MySQL schema ensured.")

def wait_for_databases(max_wait: int = 120):
    """
    Block until MongoDB and MySQL are both reachable, or until max_wait
    seconds have elapsed.  Called once at startup inside the app context.
    """
    deadline = time.time() + max_wait
    mongo_ok = mysql_ok = False

    log.info("Waiting for databases (timeout %ds)…", max_wait)

    while time.time() < deadline:
        # --- MongoDB ---
        if not mongo_ok:
            client = get_mongo_client()
            if client:
                try:
                    db = client["traffic_analytics"]
                    # Ensure collections exist
                    for col in ("traffic_data", "congestion_alerts", "users"):
                        if col not in db.list_collection_names():
                            db.create_collection(col)
                    _ensure_mongo_indexes(db)
                    mongo_ok = True
                    log.info("✅ MongoDB ready.")
                except Exception as exc:
                    log.warning("MongoDB not ready yet: %s", exc)

        # --- MySQL ---
        if not mysql_ok:
            conn = open_mysql()
            if conn:
                try:
                    _ensure_mysql_schema(conn)
                    mysql_ok = True
                    log.info("✅ MySQL ready.")
                finally:
                    conn.close()

        if mongo_ok and mysql_ok:
            log.info("All databases ready — serving requests.")
            return

        time.sleep(3)

    # Warn but don't crash — endpoints degrade gracefully
    log.warning(
        "⚠️  Startup timeout: mongo_ok=%s mysql_ok=%s. "
        "Endpoints will return 503 until DBs come up.",
        mongo_ok, mysql_ok,
    )

# Run once at startup
with app.app_context():
    wait_for_databases()

# ===========================================================================
# STATIC FILE SERVING
# ===========================================================================

@app.route("/")
def index():
    return send_from_directory(".", "login.html")

@app.route("/<path:filename>")
def serve_static(filename):
    return send_from_directory(".", filename)

# ===========================================================================
# AUTH
# ===========================================================================

@app.route("/api/signup", methods=["POST"])
@require_json
def signup():
    conn = get_mysql()
    if not conn:
        return jsonify({"error": "Database unavailable. Try again shortly."}), 503

    data     = request.get_json()
    username = (data.get("username") or "").strip()
    email    = (data.get("email")    or "").strip().lower()
    password = (data.get("password") or "")

    if not username or not email or not password:
        return jsonify({"error": "username, email and password are required"}), 400
    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    if "@" not in email:
        return jsonify({"error": "Invalid email address"}), 400

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE username=%s OR email=%s",
                (username, email),
            )
            if cur.fetchone():
                return jsonify({"error": "Username or email already taken"}), 409

            cur.execute(
                "INSERT INTO users (username, email, password_hash, is_admin) VALUES (%s,%s,%s,%s)",
                (username, email, pw_hash, False),
            )
            user_id = cur.lastrowid
    except pymysql.Error as exc:
        log.error("signup DB error: %s", exc)
        return jsonify({"error": "Database error"}), 500

    # Mirror into MongoDB (optional — best-effort only)
    db = get_db()
    if db is not None:
        try:
            db.users.insert_one({
                "username": username, "email": email,
                "is_admin": False, "mysql_user_id": user_id,
                "created_at": now_utc(),
            })
        except Exception as exc:
            log.warning("Mongo user mirror failed (non-fatal): %s", exc)

    return jsonify({"message": "Account created successfully", "user_id": user_id}), 201


@app.route("/api/login", methods=["POST"])
@require_json
def login():
    conn = get_mysql()
    if not conn:
        return jsonify({"error": "Database unavailable. Try again shortly."}), 503

    data     = request.get_json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "")

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username=%s", (username,))
            user = cur.fetchone()
    except pymysql.Error as exc:
        log.error("login DB error: %s", exc)
        return jsonify({"error": "Database error"}), 500

    if not user or not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
        return jsonify({"error": "Invalid username or password"}), 401

    return jsonify({
        "message": "Login successful",
        "user": {
            "id":       user["id"],
            "username": user["username"],
            "email":    user["email"],
            "is_admin": bool(user["is_admin"]),
        },
    }), 200


@app.route("/api/logout", methods=["POST"])
def logout():
    return jsonify({"message": "Logged out"}), 200

# ===========================================================================
# TRAFFIC DATA
# ===========================================================================

@app.route("/api/traffic-data", methods=["GET"])
def get_traffic_data():
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        limit = min(max(int(request.args.get("limit", 20)), 1), 100)
        page  = max(int(request.args.get("page",  1)),  1)
        skip  = (page - 1) * limit

        docs  = list(db.traffic_data.find().sort("created_at", -1).skip(skip).limit(limit))
        total = db.traffic_data.count_documents({})

        return jsonify({
            "data":        [serialize_doc(d) for d in docs],
            "total":       total,
            "page":        page,
            "limit":       limit,
            "total_pages": max(1, (total + limit - 1) // limit),
        })
    except Exception as exc:
        log.error("get_traffic_data: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/traffic-data/recent", methods=["GET"])
def get_recent_traffic_data():
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        docs = list(db.traffic_data.find().sort("created_at", -1).limit(20))
        return jsonify([serialize_doc(d) for d in docs])
    except Exception as exc:
        log.error("get_recent_traffic_data: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/congestion-alerts", methods=["GET"])
def get_congestion_alerts():
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        alerts = list(
            db.congestion_alerts.find({"resolved": False})
            .sort("timestamp", -1)
            .limit(10)
        )
        return jsonify([serialize_doc(a) for a in alerts])
    except Exception as exc:
        log.error("get_congestion_alerts: %s", exc)
        return jsonify({"error": str(exc)}), 500

# ===========================================================================
# STATISTICS & HEALTH
# ===========================================================================

@app.route("/api/health", methods=["GET"])
def health_check():
    mongo_ok = get_mongo_client() is not None

    mysql_ok = False
    conn = get_mysql()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            mysql_ok = True
        except Exception:
            pass

    return jsonify({
        "flask_app": "healthy",
        "mongodb":   "connected"    if mongo_ok else "disconnected",
        "mysql":     "connected"    if mysql_ok else "disconnected",
        "timestamp": now_utc().isoformat(),
    })


@app.route("/api/stats", methods=["GET"])
def get_stats():
    stats = {
        "total_users": 0, "admin_users": 0, "regular_users": 0,
        "total_traffic_records": 0, "total_congestion_alerts": 0,
        "active_congestion_alerts": 0, "recent_activity": 0,
    }

    conn = get_mysql()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM users")
                stats["total_users"] = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE is_admin=TRUE")
                stats["admin_users"] = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE is_admin=FALSE")
                stats["regular_users"] = cur.fetchone()["c"]
        except pymysql.Error as exc:
            log.error("stats MySQL: %s", exc)

    db = get_db()
    if db is not None:
        try:
            stats["total_traffic_records"]    = db.traffic_data.count_documents({})
            stats["total_congestion_alerts"]  = db.congestion_alerts.count_documents({})
            stats["active_congestion_alerts"] = db.congestion_alerts.count_documents({"resolved": False})
            hour_ago = now_utc() - timedelta(hours=1)
            stats["recent_activity"] = db.traffic_data.count_documents({"created_at": {"$gte": hour_ago}})
        except Exception as exc:
            log.error("stats MongoDB: %s", exc)

    return jsonify(stats)


@app.route("/api/real-time-stats", methods=["GET"])
def get_real_time_stats():
    empty = {
        "total_vehicles": 0, "avg_speed": 0,
        "active_roads": 0, "vehicle_types": {},
        "timestamp": now_utc().isoformat(),
    }

    db = get_db()
    if db is None:
        return jsonify({**empty, "message": "MongoDB unavailable"})

    try:
        five_min_ago = now_utc() - timedelta(minutes=5)
        docs = list(db.traffic_data.find({"created_at": {"$gte": five_min_ago}}).limit(500))

        # Fallback: use the 20 most recent records regardless of age
        if not docs:
            docs = list(db.traffic_data.find().sort("created_at", -1).limit(20))

        if not docs:
            return jsonify({**empty, "message": "No data yet — use the simulator"})

        total      = len(docs)
        avg_speed  = sum(d.get("speed", 0) for d in docs) / total
        roads      = {d.get("road_name", "Unknown") for d in docs}
        vtypes: dict[str, int] = {}
        for d in docs:
            vt = d.get("vehicle_type", "unknown")
            vtypes[vt] = vtypes.get(vt, 0) + 1

        return jsonify({
            "total_vehicles": total,
            "avg_speed":      round(avg_speed, 1),
            "active_roads":   len(roads),
            "vehicle_types":  vtypes,
            "timestamp":      now_utc().isoformat(),
        })
    except Exception as exc:
        log.error("real_time_stats: %s", exc)
        return jsonify({"error": str(exc)}), 500

# ===========================================================================
# ADMIN
# ===========================================================================

@app.route("/api/admin/signup", methods=["POST"])
@require_json
def admin_signup():
    conn = get_mysql()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    data         = request.get_json()
    username     = (data.get("username")     or "").strip()
    email        = (data.get("email")        or "").strip().lower()
    password     = (data.get("password")     or "")
    admin_secret = (data.get("admin_secret") or "")

    if not username or not email or not password:
        return jsonify({"error": "All fields are required"}), 400
    if admin_secret != ADMIN_SECRET:
        return jsonify({"error": "Invalid admin secret"}), 403

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE username=%s OR email=%s",
                (username, email),
            )
            if cur.fetchone():
                return jsonify({"error": "User already exists"}), 409

            cur.execute(
                "INSERT INTO users (username, email, password_hash, is_admin) VALUES (%s,%s,%s,%s)",
                (username, email, pw_hash, True),
            )
            user_id = cur.lastrowid
    except pymysql.Error as exc:
        log.error("admin_signup DB: %s", exc)
        return jsonify({"error": "Database error"}), 500

    db = get_db()
    if db is not None:
        try:
            db.users.insert_one({
                "username": username, "email": email,
                "is_admin": True, "mysql_user_id": user_id,
                "created_at": now_utc(),
            })
        except Exception:
            pass

    return jsonify({"message": "Admin account created", "user_id": user_id}), 201


@app.route("/api/admin/users", methods=["GET"])
def get_all_users():
    conn = get_mysql()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, email, is_admin,
                       DATE_FORMAT(created_at, '%%Y-%%m-%%d %%H:%%i:%%s') AS created_at
                FROM users
                ORDER BY created_at DESC
            """)
            return jsonify(cur.fetchall())
    except pymysql.Error as exc:
        log.error("get_all_users: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/admin/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    conn = get_mysql()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, is_admin FROM users WHERE id=%s", (user_id,))
            user = cur.fetchone()
            if not user:
                return jsonify({"error": "User not found"}), 404
            if user["is_admin"]:
                return jsonify({"error": "Cannot delete admin users"}), 403
            cur.execute("DELETE FROM users WHERE id=%s", (user_id,))
    except pymysql.Error as exc:
        log.error("delete_user: %s", exc)
        return jsonify({"error": str(exc)}), 500

    db = get_db()
    if db is not None:
        try:
            db.users.delete_one({"mysql_user_id": user_id})
        except Exception:
            pass

    return jsonify({"message": "User deleted"})


@app.route("/api/admin/users/<int:user_id>", methods=["PUT"])
@require_json
def update_user(user_id):
    conn = get_mysql()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    data = request.get_json()
    fields, values = [], []

    if data.get("username"):
        fields.append("username=%s"); values.append(data["username"].strip())
    if data.get("email"):
        fields.append("email=%s");    values.append(data["email"].strip().lower())
    if "is_admin" in data:
        fields.append("is_admin=%s"); values.append(bool(data["is_admin"]))

    if not fields:
        return jsonify({"error": "Nothing to update"}), 400

    values.append(user_id)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE id=%s", (user_id,))
            if not cur.fetchone():
                return jsonify({"error": "User not found"}), 404
            cur.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=%s", values)
    except pymysql.Error as exc:
        log.error("update_user: %s", exc)
        return jsonify({"error": str(exc)}), 500

    return jsonify({"message": "User updated"})


@app.route("/api/admin/traffic-data", methods=["GET"])
def get_all_traffic_data():
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        page  = max(int(request.args.get("page",  1)), 1)
        limit = min(max(int(request.args.get("limit", 20)), 1), 100)
        skip  = (page - 1) * limit

        docs  = list(db.traffic_data.find().sort("created_at", -1).skip(skip).limit(limit))
        total = db.traffic_data.count_documents({})

        return jsonify({
            "data":        [serialize_doc(d) for d in docs],
            "total":       total,
            "page":        page,
            "limit":       limit,
            "total_pages": max(1, (total + limit - 1) // limit),
        })
    except Exception as exc:
        log.error("get_all_traffic_data: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/admin/traffic-data/<string:data_id>", methods=["DELETE"])
def delete_traffic_data(data_id):
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        oid = ObjectId(data_id)
    except Exception:
        return jsonify({"error": "Invalid record ID"}), 400

    try:
        result = db.traffic_data.delete_one({"_id": oid})
        if result.deleted_count == 0:
            return jsonify({"error": "Record not found"}), 404
        return jsonify({"message": "Record deleted"})
    except Exception as exc:
        log.error("delete_traffic_data: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/admin/stats", methods=["GET"])
def admin_stats():
    stats = {
        "total_users": 0, "admin_users": 0, "regular_users": 0,
        "total_traffic_records": 0, "total_congestion_alerts": 0,
        "active_congestion_alerts": 0, "top_roads": [], "vehicle_distribution": [],
    }

    conn = get_mysql()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM users")
                stats["total_users"] = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE is_admin=TRUE")
                stats["admin_users"] = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE is_admin=FALSE")
                stats["regular_users"] = cur.fetchone()["c"]
        except pymysql.Error as exc:
            log.error("admin_stats MySQL: %s", exc)

    db = get_db()
    if db is not None:
        try:
            stats["total_traffic_records"]    = db.traffic_data.count_documents({})
            stats["total_congestion_alerts"]  = db.congestion_alerts.count_documents({})
            stats["active_congestion_alerts"] = db.congestion_alerts.count_documents({"resolved": False})

            top_roads = list(db.traffic_data.aggregate([
                {"$group": {"_id": "$road_name", "count": {"$sum": 1}}},
                {"$sort":  {"count": -1}},
                {"$limit": 5},
            ]))
            stats["top_roads"] = [{"road": r["_id"], "count": r["count"]} for r in top_roads]

            vdist = list(db.traffic_data.aggregate([
                {"$group": {"_id": "$vehicle_type", "count": {"$sum": 1}}},
                {"$sort":  {"count": -1}},
            ]))
            stats["vehicle_distribution"] = [{"type": v["_id"], "count": v["count"]} for v in vdist]
        except Exception as exc:
            log.error("admin_stats MongoDB: %s", exc)

    return jsonify(stats)

# ===========================================================================
# SIMULATION & MAP
# ===========================================================================

ROADS = [
    {"road_id": "RD001", "name": "Main Street",
     "coords": [(40.7500, -74.0050), (40.7600, -73.9950)]},
    {"road_id": "RD002", "name": "Broadway",
     "coords": [(40.7550, -74.0100), (40.7650, -74.0000)]},
    {"road_id": "RD003", "name": "5th Avenue",
     "coords": [(40.7450, -74.0050), (40.7550, -73.9950)]},
    {"road_id": "RD004", "name": "Park Avenue",
     "coords": [(40.7480, -74.0150), (40.7580, -74.0050)]},
    {"road_id": "RD005", "name": "Madison Avenue",
     "coords": [(40.7520, -74.0080), (40.7620, -73.9980)]},
]

def _make_traffic_doc(data: dict | None = None) -> dict:
    """Build a single traffic record, optionally seeded from `data`."""
    data  = data or {}
    speed = float(data.get("speed", random.uniform(10, 80)))
    road  = random.choice(ROADS)
    s, e  = road["coords"]
    t     = random.random()
    lat   = round(s[0] + (e[0] - s[0]) * t + random.uniform(-0.001, 0.001), 6)
    lon   = round(s[1] + (e[1] - s[1]) * t + random.uniform(-0.001, 0.001), 6)
    ts    = now_utc()
    return {
        "vehicle_id":       data.get("vehicle_id", f"V{random.randint(1, 200):04d}"),
        "timestamp":        ts.isoformat(),
        "created_at":       ts,                  # datetime — for index queries
        "latitude":         data.get("latitude",  lat),
        "longitude":        data.get("longitude", lon),
        "speed":            round(speed, 1),
        "road_id":          data.get("road_id",   road["road_id"]),
        "road_name":        data.get("road_name", road["name"]),
        "vehicle_type":     data.get("vehicle_type",
                                     random.choice(["car","truck","bus","motorcycle"])),
        "congestion_level": "low" if speed > 40 else "medium" if speed > 20 else "high",
    }


@app.route("/api/simulate-data", methods=["POST"])
@require_json
def simulate_data():
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        doc    = _make_traffic_doc(request.get_json())
        result = db.traffic_data.insert_one(doc)
        return jsonify({
            "message":     "Record added",
            "inserted_id": str(result.inserted_id),
            "data":        serialize_doc(doc),
        }), 201
    except Exception as exc:
        log.error("simulate_data: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/bulk-simulate", methods=["POST"])
@require_json
def bulk_simulate():
    db = get_db()
    if db is None:
        return jsonify({"error": "MongoDB unavailable"}), 503

    try:
        body  = request.get_json() or {}
        count = min(max(int(body.get("count", 20)), 1), 200)
        docs  = [_make_traffic_doc() for _ in range(count)]
        result = db.traffic_data.insert_many(docs)
        return jsonify({"message": f"Added {len(result.inserted_ids)} records"}), 201
    except Exception as exc:
        log.error("bulk_simulate: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/congestion-map", methods=["GET"])
def get_congestion_map():
    base = {
        "congestion_alerts":  [],
        "vehicle_positions":  [],
        "last_updated":       now_utc().isoformat(),
    }

    db = get_db()
    if db is None:
        return jsonify(base)

    try:
        alerts   = list(db.congestion_alerts.find({"resolved": False}).sort("timestamp", -1).limit(10))
        vehicles = list(db.traffic_data.find().sort("created_at", -1).limit(60))

        base["congestion_alerts"] = [
            {
                "road_id":       a.get("road_id"),
                "road_name":     a.get("road_name"),
                "severity":      a.get("severity", "medium"),
                "avg_speed":     a.get("avg_speed", 0),
                "vehicle_count": a.get("vehicle_count", 0),
                "cause":         a.get("cause", "unknown"),
                "timestamp":     a.get("timestamp"),
            }
            for a in alerts
        ]

        base["vehicle_positions"] = [
            {
                "vehicle_id":      v.get("vehicle_id"),
                "latitude":        v.get("latitude"),
                "longitude":       v.get("longitude"),
                "speed":           v.get("speed", 0),
                "road_name":       v.get("road_name", "Unknown"),
                "vehicle_type":    v.get("vehicle_type", "car"),
                "congestion_level": v.get("congestion_level", "low"),
            }
            for v in vehicles
        ]

        return jsonify(base)
    except Exception as exc:
        log.error("congestion_map: %s", exc)
        return jsonify({"error": str(exc)}), 500

# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    log.info("🚀 Smart City Traffic Analytics starting on :5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
