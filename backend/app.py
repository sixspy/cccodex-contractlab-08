
import hashlib, secrets, sqlite3, threading
from datetime import datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# ---------------------------------------------------------------------------
# 主题隔离口径（甲路径，单一口径，禁止与乙路径条件更新混用）
# 写入前按主题加锁：锁内重新核对主题与契约，不一致即拒绝、零写入并登记；
# 同主题并发后到者不排队，只登记主题争用。
# 口径一经选定即写入主题隔离簿（isolation_policy），当天不许换。
# ---------------------------------------------------------------------------
ISOLATION_APPROACH = "写入前按主题加锁"
ALLOWED_APPROACHES = ("写入前按主题加锁", "条件更新主题版本双命中")

_SUBJECT_LOCK_GUARD = threading.Lock()
_SUBJECT_LOCKS: dict[str, threading.Lock] = {}
_SUBJECT_BUSY: set[str] = set()


def subject_lock(subject: str) -> threading.Lock:
    with _SUBJECT_LOCK_GUARD:
        lk = _SUBJECT_LOCKS.get(subject)
        if lk is None:
            lk = threading.Lock()
            _SUBJECT_LOCKS[subject] = lk
        return lk


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def record_contention(conn, subject: str, contract_version: str | None, kind: str, detail: str):
    conn.execute(
        "INSERT INTO topic_contentions(subject,contract_version,kind,detail,created_at) VALUES (?,?,?,?,?)",
        (subject, contract_version, kind, detail, datetime.now().isoformat(timespec="seconds")),
    )


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS isolation_policy(
          id INTEGER PRIMARY KEY CHECK (id = 1),
          approach TEXT NOT NULL,
          chosen_at TEXT NOT NULL,
          chosen_by TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS topic_contentions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          contract_version TEXT,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3", "订单入站", "严格", "正式", "启用", "order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow", "订单入站", "兼容", "影子", "试跑", "order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2", "库存入站", "严格", "正式", "停用", "sku,qty"))
        # 主题隔离簿：首写落定甲路径；库里若已记别的口径，直接拦下（混用禁止）
        row = conn.execute("SELECT approach FROM isolation_policy WHERE id=1").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO isolation_policy(id,approach,chosen_at,chosen_by) VALUES (?,?,?,?)",
                (1, ISOLATION_APPROACH, datetime.now().isoformat(timespec="seconds"), "system"),
            )
        elif row["approach"] != ISOLATION_APPROACH:
            raise RuntimeError(
                f"主题隔离簿口径={row['approach']} 与现行口径={ISOLATION_APPROACH} 不一致，"
                "主题口径偏了，禁止混用主题支"
            )


@app.on_event("startup")
def _s():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n,
            "db": str(DB_PATH), "isolation_approach": ISOLATION_APPROACH}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None),
              subject: str | None = Query(default=None)):
    """版本目录：可按主题隔离，绝不把别的主题契约串进来。"""
    require_user(authorization)
    with connect() as c:
        if subject:
            rows = c.execute("SELECT * FROM contracts WHERE subject=? ORDER BY id", (subject,)).fetchall()
        else:
            rows = c.execute("SELECT * FROM contracts ORDER BY id").fetchall()
        return {"items": [dict(r) for r in rows]}


class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present


class LedgerIn(BaseModel):
    approach: str


@app.post("/api/isolation/ledger")
def choose_approach(body: LedgerIn, authorization: str | None = Header(default=None)):
    """主题隔离簿登记：口径当天不许换，偏离即拦下并记主题口径偏了。"""
    user = require_user(authorization)
    if body.approach not in ALLOWED_APPROACHES:
        raise HTTPException(400, "未知主题隔离口径")
    with connect() as c:
        row = c.execute("SELECT approach FROM isolation_policy WHERE id=1").fetchone()
        current = row["approach"] if row else ISOLATION_APPROACH
        if row is None:
            c.execute(
                "INSERT INTO isolation_policy(id,approach,chosen_at,chosen_by) VALUES (?,?,?,?)",
                (1, body.approach, datetime.now().isoformat(timespec="seconds"), user),
            )
            return {"approach": body.approach, "locked": True}
        if body.approach != current:
            record_contention(c, "-", None, "主题口径偏了",
                              f"申请口径={body.approach} 与隔离簿口径={current} 冲突，当天不许换")
            c.commit()
            raise HTTPException(409, f"主题隔离簿已落定『{current}』，当天不许换")
        return {"approach": current, "locked": True}


@app.get("/api/isolation/ledger")
def get_ledger(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM isolation_policy WHERE id=1").fetchone()
        approach = row["approach"] if row else ISOLATION_APPROACH
        return {"approach": approach,
                "locked": True,
                "chosen_at": row["chosen_at"] if row else None,
                "chosen_by": row["chosen_by"] if row else None}


@app.get("/api/contentions")
def contentions(authorization: str | None = Header(default=None),
                subject: str | None = Query(default=None)):
    """主题争用/口径偏离登记簿（策略附页），可按主题隔离。"""
    require_user(authorization)
    with connect() as c:
        if subject:
            rows = c.execute(
                "SELECT * FROM topic_contentions WHERE subject=? ORDER BY id DESC", (subject,)
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM topic_contentions ORDER BY id DESC").fetchall()
        return {"items": [dict(r) for r in rows]}


@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    lk = subject_lock(body.subject)

    # 同主题两名值班同时抢：后到者不排队，只登记主题争用、零写入
    if body.subject in _SUBJECT_BUSY:
        with connect() as c:
            record_contention(c, body.subject, None, "同主题争用",
                              f"值班 {user} 后到，同主题入库进行中，后到只登记不写入")
            c.commit()
        raise HTTPException(409, "主题争用：同主题入库进行中，本单后到，已登记争用且零写入")

    with lk:
        _SUBJECT_BUSY.add(body.subject)
        try:
            # 甲路径：写入前按主题加锁，锁内一次连接内完成核对与落库
            with connect() as c:
                ctr = c.execute(
                    "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' "
                    "ORDER BY id DESC LIMIT 1",
                    (body.subject,),
                ).fetchone()
                if not ctr:
                    # 禁止无启用契约仍写通过：直接拒绝，不留任何批次行
                    raise HTTPException(404, "无启用正式契约")

                now = datetime.now().isoformat(timespec="seconds")
                need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
                missing = sorted(need - fields)

                # 插入前再按主题+版本双命中核对契约目录，防交错期间目录改挂
                fresh = c.execute(
                    "SELECT version,subject,status FROM contracts "
                    "WHERE version=? AND subject=? AND track='正式' AND status='启用'",
                    (ctr["version"], body.subject),
                ).fetchone()
                if fresh is None:
                    record_contention(
                        c, body.subject, ctr["version"], "主题口径偏了",
                        f"契约 {ctr['version']} 与主题 {body.subject} 已失配，拒绝串写、零写入",
                    )
                    c.commit()
                    raise HTTPException(409, "主题口径偏了：契约目录已变更，本单拒绝串写且零写入")

                def insert_batch(result: str, detail: str):
                    cur = c.execute(
                        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,"
                        "result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], result, detail, now),
                    )
                    return cur.lastrowid

                if missing:
                    if ctr["mode"] == "兼容":
                        batch_id = insert_batch("缓冲", "缺字段:" + ",".join(missing))
                        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)",
                                  (batch_id, "缺字段进双写缓冲"))
                        c.commit()
                        return {"result": "缓冲", "version": ctr["version"],
                                "missing": missing, "batch_id": batch_id}
                    batch_id = insert_batch("拒收", "缺字段:" + ",".join(missing))
                    c.commit()
                    return {"result": "拒收", "version": ctr["version"],
                            "missing": missing, "batch_id": batch_id}

                batch_id = insert_batch("通过", "字段齐全")
                c.commit()
                return {"result": "通过", "version": ctr["version"], "batch_id": batch_id}
        finally:
            _SUBJECT_BUSY.discard(body.subject)


@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None),
            subject: str | None = Query(default=None)):
    """入站列表：带主题条件时只出本主题，不带则全量，绝不串题展示。"""
    require_user(authorization)
    with connect() as c:
        if subject:
            rows = c.execute(
                "SELECT * FROM ingress_batches WHERE subject=? ORDER BY id DESC", (subject,)
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/api/ingress/{batch_id}")
def ingress_detail(batch_id: int,
                   authorization: str | None = Header(default=None),
                   subject: str | None = Query(default=None)):
    """详情：主题对不上即视为串题查询，挡下。"""
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM ingress_batches WHERE id=?", (batch_id,)).fetchone()
        if row is None or (subject is not None and row["subject"] != subject):
            raise HTTPException(404, "批次不存在或主题不属本主题目录")
        return dict(row)


@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None),
            subject: str | None = Query(default=None)):
    require_user(authorization)
    with connect() as c:
        if subject:
            rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b
                                JOIN ingress_batches i ON i.id=b.batch_id
                                WHERE i.subject=? ORDER BY b.id DESC""", (subject,)).fetchall()
        else:
            rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b
                                JOIN ingress_batches i ON i.id=b.batch_id
                                ORDER BY b.id DESC""").fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None),
              subject: str | None = Query(default=None)):
    """对账账册：可按主题隔离核算；带主题条件时账册只认本主题行。"""
    require_user(authorization)
    with connect() as c:
        if subject:
            total = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE subject=?", (subject,)).fetchone()["c"]
            passed = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE result='通过' AND subject=?",
                (subject,)).fetchone()["c"]
            buffered = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲' AND subject=?",
                (subject,)).fetchone()["c"]
            rejected = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收' AND subject=?",
                (subject,)).fetchone()["c"]
            buf_rows = c.execute("""SELECT COUNT(*) c FROM buffers b
                                    JOIN ingress_batches i ON i.id=b.batch_id
                                    WHERE i.subject=?""", (subject,)).fetchone()["c"]
            contentions = c.execute(
                "SELECT COUNT(*) c FROM topic_contentions WHERE subject=?", (subject,)
            ).fetchone()["c"]
        else:
            total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
            passed = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
            buffered = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
            rejected = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
            buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
            contentions = c.execute("SELECT COUNT(*) c FROM topic_contentions").fetchone()["c"]
    return {"ingress_total": total, "passed": passed, "buffered": buffered,
            "rejected": rejected, "buffer_rows": buf_rows, "contention_rows": contentions,
            "scoped_subject": subject,
            "aligned": buffered == buf_rows}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
