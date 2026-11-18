"""多主题并发入库串题复现脚本（修复前证据）。

运行：.venv/bin/python tests/repro_cross_subject.py

场景：
1. 交错双主题：两个线程分别投「订单入站」「库存入站」，在契约 SELECT 处设
   Barrier(2)，双方都查到契约后、插入前，栅栏动作把契约目录里 v3 的主题改挂
   到「库存入站」。现行代码 SELECT 后不再核对，插入静默成功 —— 入站批次主题
   与契约目录对不上，即串题。
2. 同主题双值班同时抢：现行代码两条都写「通过」，无任何主题争用登记。
3. 无启用契约：现行行为应为拒绝且零写入（回归守卫）。
"""
import os
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))

import app  # noqa: E402

# 免登录：直接替换鉴权
app.require_user = lambda authorization=None: "admin"

HOOK_BARRIER: threading.Barrier | None = None


def _tune(c):
    c.execute("PRAGMA busy_timeout=15000")
    c.execute("PRAGMA journal_mode=WAL")
    return c


class HookedConn:
    def __init__(self, conn):
        self._c = _tune(conn)

    def execute(self, sql, params=()):
        cur = self._c.execute(sql, params)
        if HOOK_BARRIER is not None and "FROM contracts WHERE subject=" in sql:
            # SELECT 已完成、INSERT 之前才汇合，保证两线程都拿到了旧契约
            HOOK_BARRIER.wait(timeout=10)
        return cur

    def __getattr__(self, name):
        return getattr(self._c, name)

    def __enter__(self):
        self._c.__enter__()
        return self

    def __exit__(self, *a):
        return self._c.__exit__(*a)


def raw_conn():
    c = sqlite3.connect(app.DB_PATH, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=15000")
    return c


def seed_second_subject():
    with raw_conn() as c:
        c.execute(
            "INSERT INTO contracts(version,subject,mode,track,status,required_fields) "
            "VALUES (?,?,?,?,?,?)",
            ("v5", "库存入站", "严格", "正式", "启用", "sku,qty"),
        )


def catalog_remap_v3():
    """栅栏动作：目录切换，v3 由订单入站改挂库存入站。"""
    c = sqlite3.connect(app.DB_PATH, timeout=15)
    c.execute("PRAGMA busy_timeout=15000")
    c.execute("UPDATE contracts SET subject='库存入站' WHERE version='v3'")
    c.commit()
    c.close()


def call_validate(subject, fields, box, name):
    try:
        r = app.validate(app.IngressIn(subject=subject, payload_fields=fields))
        box[name] = ("ok", r)
    except app.HTTPException as e:
        box[name] = ("http", e.status_code, e.detail)
    except Exception as e:  # noqa: BLE001
        box[name] = ("err", repr(e))


def main():
    tmp = tempfile.mkdtemp(prefix="repro-topic-")
    app.DB_PATH = os.path.join(tmp, "repro.db")
    app.SESSIONS.clear()
    app.init_db()
    seed_second_subject()

    orig_connect = app.connect
    app.connect = lambda: HookedConn(orig_connect())

    failures = []

    # ---- 场景 1：交错双主题 + 目录切换 ----
    print("=" * 64)
    print("场景1 交错双主题：SELECT 后目录切换，v3 改挂库存入站")
    print("=" * 64)
    global HOOK_BARRIER
    HOOK_BARRIER = threading.Barrier(2, action=catalog_remap_v3)
    box: dict = {}
    t1 = threading.Thread(target=call_validate,
                          args=("订单入站", "order_id,amount,currency", box, "订单"))
    t2 = threading.Thread(target=call_validate,
                          args=("库存入站", "sku,qty", box, "库存"))
    t1.start(); t2.start(); t1.join(); t2.join()
    HOOK_BARRIER = None
    print("线程返回：", box)

    with raw_conn() as c:
        rows = c.execute("""
            SELECT i.id, i.subject AS batch_subject, i.contract_version,
                   ct.subject AS catalog_subject
            FROM ingress_batches i
            LEFT JOIN contracts ct ON ct.version = i.contract_version
            ORDER BY i.id
        """).fetchall()
    print("\n入站批次 vs 契约目录：")
    crossed = []
    for r in rows:
        mark = "串题!" if r["catalog_subject"] != r["batch_subject"] else "对齐"
        if r["catalog_subject"] != r["batch_subject"]:
            crossed.append(dict(r))
        print(f"  批次#{r['id']} 批次主题={r['batch_subject']} "
              f"契约={r['contract_version']} 目录主题={r['catalog_subject']} -> {mark}")
    if crossed:
        failures.append(("交错双主题静默串写", crossed))
        print(f"\n[见底] {len(crossed)} 条批次主题与契约目录对不上，且接口仍返回成功\n")
    else:
        print("\n[未复现] 无串题行\n")

    # 恢复目录，供后续场景
    with sqlite3.connect(app.DB_PATH) as c:
        c.execute("UPDATE contracts SET subject='订单入站' WHERE version='v3'")
        c.commit()

    # ---- 场景 2：同主题双值班同时抢 ----
    print("=" * 64)
    print("场景2 同主题两名值班同时抢入库")
    print("=" * 64)
    start = threading.Barrier(2)
    box2: dict = {}

    def clerk(name):
        start.wait(timeout=10)
        call_validate("订单入站", "order_id,amount,currency", box2, name)

    w1 = threading.Thread(target=clerk, args=("值班甲",))
    w2 = threading.Thread(target=clerk, args=("值班乙",))
    w1.start(); w2.start(); w1.join(); w2.join()
    print("两线程返回：", box2)
    with raw_conn() as c:
        n_pass = c.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE subject='订单入站' AND result='通过'"
        ).fetchone()["n"]
        has_contention_table = c.execute(
            "SELECT COUNT(*) n FROM sqlite_master WHERE type='table' AND name='topic_contentions'"
        ).fetchone()["n"]
    print(f"订单入站『通过』批次数：{n_pass}；主题争用登记表存在：{bool(has_contention_table)}")
    if n_pass >= 2 and not has_contention_table:
        failures.append(("同主题并发无排队/无争用登记", {"通过批次": n_pass}))
        print("[见底] 两人同主题同时抢，两条都静默写通过，没有争用登记\n")
    else:
        print("[未复现]\n")

    # ---- 场景 3：无启用契约不许写通过 ----
    print("=" * 64)
    print("场景3 停用全部正式契约后提交")
    print("=" * 64)
    with sqlite3.connect(app.DB_PATH) as c:
        c.execute("UPDATE contracts SET status='停用' WHERE subject='库存入站'")
        c.commit()
    box3: dict = {}
    with raw_conn() as c:
        before = c.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE subject='库存入站' AND result='通过'"
        ).fetchone()["n"]
    call_validate("库存入站", "sku,qty", box3, "库存")
    with raw_conn() as c:
        after = c.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE subject='库存入站' AND result='通过'"
        ).fetchone()["n"]
    print("返回：", box3, " 库存『通过』批次：", before, "->", after)
    if after != before:
        failures.append(("无启用契约仍写通过", box3.get("库存")))
        print("[见底] 无启用契约竟然写通过\n")
    else:
        print("[守得住] 已拒绝且零通过写入\n")

    print("=" * 64)
    if failures:
        print(f"复现结论：{len(failures)} 类主题隔离缺陷见底：")
        for name, ev in failures:
            print(f"  - {name}: {ev}")
        sys.exit(1)
    print("复现结论：主题隔离口径全部守得住")
    sys.exit(0)


if __name__ == "__main__":
    main()
