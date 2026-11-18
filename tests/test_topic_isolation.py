"""主题隔离失败测（先红后绿）。

正确性口径（选定后写入主题隔离簿，当天不许换，不得与乙路径混用）：
  甲路径 = 写入前按主题加锁，不一致则拒绝、零写入；同主题并发后到者
           只登记主题争用，不排队不静默写成功。

守卫口径：
  - 禁止主题串写静默成功（交错目录切换后，旧契约失配必须零写入并记争用）
  - 禁止无启用契约仍写通过
  - 入站列表、版本目录、详情、对账账册、策略附页均支持按主题隔离，不串题
"""
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import app, init_db, ISOLATION_APPROACH

HOOK_BARRIER: threading.Barrier | None = None


class HookedConn:
    def __init__(self, conn):
        self._c = conn

    def execute(self, sql, params=()):
        cur = self._c.execute(sql, params)
        if HOOK_BARRIER is not None and "FROM contracts WHERE subject=" in sql:
            HOOK_BARRIER.wait(timeout=10)
        return cur

    def __getattr__(self, name):
        return getattr(self._c, name)

    def commit(self):
        return self._c.commit()

    def __enter__(self):
        self._c.__enter__()
        return self

    def __exit__(self, *a):
        return self._c.__exit__(*a)


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    import app as mod
    monkeypatch.setattr(mod, "DB_PATH", tmp_path / "t.db")
    init_db()
    mod.SESSIONS.clear()
    # 主题锁/争用状态按库隔离，每个用例重置
    if hasattr(mod, "_SUBJECT_LOCKS"):
        mod._SUBJECT_LOCKS.clear()
    if hasattr(mod, "_SUBJECT_BUSY"):
        mod._SUBJECT_BUSY.clear()
    with sqlite3.connect(mod.DB_PATH) as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute(
            "INSERT INTO contracts(version,subject,mode,track,status,required_fields) "
            "VALUES (?,?,?,?,?,?)",
            ("v5", "库存入站", "严格", "正式", "启用", "sku,qty"),
        )
        c.commit()
    yield


@pytest.fixture()
def client():
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    c.headers.update({"Authorization": f"Bearer {tok}"})
    return c


def _raw():
    import app as mod
    c = sqlite3.connect(mod.DB_PATH, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=15000")
    return c


def _call_validate(subject, fields, box, name):
    import app as mod
    if mod.SESSIONS:
        tok = next(iter(mod.SESSIONS))
        auth = f"Bearer {tok}"
    else:
        mod.require_user = lambda authorization=None: "admin"
        auth = None
    try:
        r = mod.validate(mod.IngressIn(subject=subject, payload_fields=fields),
                         authorization=auth)
        box[name] = ("ok", r)
    except Exception as e:  # noqa: BLE001
        box[name] = ("err", type(e).__name__, getattr(e, "status_code", None), str(e))


def test_single_writer_still_passes_and_is_consistent(client):
    r = client.post("/api/ingress/validate",
                    json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 200 and r.json()["result"] == "通过"
    with _raw() as c:
        row = c.execute("""
            SELECT i.subject bsub, i.contract_version v, ct.subject csub
            FROM ingress_batches i JOIN contracts ct ON ct.version=i.contract_version
        """).fetchone()
        assert row["bsub"] == row["csub"] == "订单入站"
        assert row["v"] == "v3"


def test_no_enabled_contract_never_writes_pass(client):
    with _raw() as c:
        c.execute("UPDATE contracts SET status='停用' WHERE subject='库存入站'")
        c.commit()
    r = client.post("/api/ingress/validate",
                    json={"subject": "库存入站", "payload_fields": "sku,qty"})
    assert r.status_code == 404
    with _raw() as c:
        n = c.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE subject='库存入站' AND result='通过'"
        ).fetchone()["n"]
    assert n == 0


def test_same_subject_concurrent_later_writer_only_records_contention(client):
    import app as mod
    start = threading.Barrier(2)
    box: dict = {}

    def clerk(name):
        start.wait(timeout=10)
        _call_validate("订单入站", "order_id,amount,currency", box, name)

    t1 = threading.Thread(target=clerk, args=("甲",))
    t2 = threading.Thread(target=clerk, args=("乙",))
    t1.start(); t2.start(); t1.join(); t2.join()

    statuses = sorted((v[0], v[2] if len(v) > 2 else None) for v in box.values())
    # 一条成功，另一条必须是 409 主题争用
    assert any(v[0] == "ok" for v in box.values())
    loser = next(v for v in box.values() if v[0] != "ok")
    assert loser[2] == 409 and "争用" in loser[3]

    with _raw() as c:
        n_pass = c.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE subject='订单入站' AND result='通过'"
        ).fetchone()["n"]
        n_ct = c.execute(
            "SELECT COUNT(*) n FROM topic_contentions WHERE kind='同主题争用' AND subject='订单入站'"
        ).fetchone()["n"]
        n_bad = c.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE result='通过' AND contract_version IS NULL"
        ).fetchone()["n"]
    assert n_pass == 1          # 后到者零写入，只有一条通过
    assert n_ct == 1            # 后到者登记主题争用
    assert n_bad == 0


def test_different_subjects_concurrent_both_independent(client):
    start = threading.Barrier(2)
    box: dict = {}

    def go(name, subject, fields):
        start.wait(timeout=10)
        _call_validate(subject, fields, box, name)

    t1 = threading.Thread(target=go, args=("订单", "订单入站", "order_id,amount,currency"))
    t2 = threading.Thread(target=go, args=("库存", "库存入站", "sku,qty"))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert all(v[0] == "ok" and v[1]["result"] == "通过" for v in box.values()), box
    with _raw() as c:
        rows = c.execute("""
            SELECT i.subject bsub, ct.subject csub
            FROM ingress_batches i JOIN contracts ct ON ct.version=i.contract_version
        """).fetchall()
    assert {r["bsub"] for r in rows} == {"订单入站", "库存入站"}
    assert all(r["bsub"] == r["csub"] for r in rows)


def test_interleaved_catalog_switch_is_rejected_zero_write(monkeypatch):
    """交错双主题：SELECT 后目录把 v3 改挂库存入站，旧契约失配必须拒绝、零写入。"""
    import app as mod
    orig_connect = mod.connect
    monkeypatch.setattr(mod, "connect", lambda: HookedConn(orig_connect()))

    global HOOK_BARRIER

    def remap():
        c = sqlite3.connect(mod.DB_PATH, timeout=15)
        c.execute("PRAGMA busy_timeout=15000")
        c.execute("UPDATE contracts SET subject='库存入站' WHERE version='v3'")
        c.commit(); c.close()

    HOOK_BARRIER = threading.Barrier(2, action=remap)
    box: dict = {}
    try:
        t1 = threading.Thread(target=_call_validate,
                              args=("订单入站", "order_id,amount,currency", box, "订单"))
        t2 = threading.Thread(target=_call_validate,
                              args=("库存入站", "sku,qty", box, "库存"))
        t1.start(); t2.start(); t1.join(); t2.join()
    finally:
        HOOK_BARRIER = None


    with _raw() as c:
        crossed = c.execute("""
            SELECT COUNT(*) n FROM ingress_batches i
            JOIN contracts ct ON ct.version=i.contract_version
            WHERE i.subject != ct.subject
        """).fetchone()["n"]
        n_ct = c.execute(
            "SELECT COUNT(*) n FROM topic_contentions WHERE kind='主题口径偏了'"
        ).fetchone()["n"]
    assert crossed == 0, "存在主题串写行"
    assert n_ct >= 1, "串写被拒后必须登记主题口径偏了"
    # 被翻走的订单主题这一单不允许静默成功
    order_res = box.get("订单")
    assert order_res and order_res[0] != "ok", box


def test_read_endpoints_scoped_by_subject(client):
    """入站列表、版本目录、详情、对账账册、策略附页均不得串题。"""
    client.post("/api/ingress/validate",
                json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    client.post("/api/ingress/validate",
                json={"subject": "库存入站", "payload_fields": "sku,qty"})

    listing = client.get("/api/ingress", params={"subject": "库存入站"}).json()["items"]
    assert listing and all(i["subject"] == "库存入站" for i in listing)

    catalog = client.get("/api/contracts", params={"subject": "订单入站"}).json()["items"]
    assert catalog and all(i["subject"] == "订单入站" for i in catalog)

    bid = listing[0]["id"]
    detail = client.get(f"/api/ingress/{bid}", params={"subject": "库存入站"})
    assert detail.status_code == 200 and detail.json()["subject"] == "库存入站"
    # 拿别的主题查这条详情必须挡下
    crossed = client.get(f"/api/ingress/{bid}", params={"subject": "订单入站"})
    assert crossed.status_code in (404, 409)

    recon = client.get("/api/reconcile", params={"subject": "库存入站"}).json()
    assert recon["scoped_subject"] == "库存入站"
    assert recon["ingress_total"] == 1

    ledger = client.get("/api/isolation/ledger").json()
    assert ledger["approach"] == ISOLATION_APPROACH
    assert ledger["approach"] == "写入前按主题加锁"
    assert ledger["locked"] is True


def test_ledger_rejects_approach_switch_same_day(client):
    r = client.post("/api/isolation/ledger", json={"approach": "条件更新主题版本双命中"})
    assert r.status_code == 409 and "不许换" in r.json()["detail"]
    with _raw() as c:
        rows = c.execute("SELECT DISTINCT approach FROM isolation_policy").fetchall()
    assert len(rows) == 1 and rows[0]["approach"] == "写入前按主题加锁"


def test_contention_register_listed_and_reconcile_aligned(client):
    start = threading.Barrier(2)
    box: dict = {}

    def clerk(name):
        start.wait(timeout=10)
        _call_validate("订单入站", "order_id,amount,currency", box, name)

    t1 = threading.Thread(target=clerk, args=("甲",))
    t2 = threading.Thread(target=clerk, args=("乙",))
    t1.start(); t2.start(); t1.join(); t2.join()

    r = client.get("/api/contentions")
    assert r.status_code == 200
    items = r.json()["items"]
    assert items and all(it["subject"] == "订单入站" for it in items)
    recon = client.get("/api/reconcile").json()
    assert recon["contention_rows"] == len(items)
    assert recon["aligned"] is True
