"""Durable per-channel bulk jobs. New jobs start in preview and do not transfer."""
from __future__ import annotations

import json

from tg2cloud.channel_bulk.dedup import classify
from tg2cloud.channel_bulk.inventory import exclusions, items
from tg2cloud.channel_bulk.ledger import Refusal, attestation, attested_remaining
from tg2cloud.channel_bulk.policy import canonical_manifest, selected
from tg2cloud.channel_bulk.schema import ensure_db

OPEN_STATES = ("preview", "paused")


def _controls(row: dict) -> dict:
    return json.loads(row["controls"])


def _whitelist(row: dict) -> list[str]:
    return json.loads(row["whitelist"])


def get_job(db, job_id: int) -> dict:
    with db._lock:
        row = db._conn.execute("SELECT * FROM channel_bulk_jobs WHERE id=?", (int(job_id),)).fetchone()
    if row is None:
        raise KeyError("批量作业不存在")
    result = dict(row)
    result["controls"] = _controls(result)
    result["whitelist"] = _whitelist(result)
    return result


def _save_controls(db, job_id: int, controls: dict, whitelist: list[str] | None, now: float) -> None:
    with db._lock:
        if whitelist is None:
            db._conn.execute(
                "UPDATE channel_bulk_jobs SET controls=?, manifest_hash='', updated_at=? WHERE id=? AND state IN ('preview','paused')",
                (json.dumps(controls, sort_keys=True), now, int(job_id)),
            )
        else:
            db._conn.execute(
                """UPDATE channel_bulk_jobs SET controls=?, whitelist=?, manifest_hash='', updated_at=?
                   WHERE id=? AND state IN ('preview','paused')""",
                (json.dumps(controls, sort_keys=True), json.dumps(whitelist), now, int(job_id)),
            )


def open_job(db, inventory_id: int, alias: str, *, now: float) -> dict:
    """Create a preview job. Does not read Telegram or enqueue transfers."""
    if not isinstance(alias, str) or not 1 <= len(alias) <= 32 or any(ord(c) < 32 or c in "/:%" for c in alias):
        raise ValueError("目录别名无效")
    ensure_db(db)
    with db._lock:
        inventory = db._conn.execute(
            "SELECT * FROM channel_inventory WHERE id=?", (int(inventory_id),)
        ).fetchone()
    if inventory is None:
        raise KeyError("盘点不存在")
    controls = {
        "min_duration": 30,
        "allow": [],
        "daily_bytes": 0,
        "monthly_bytes": 0,
        "down_bps": 0,
        "up_bps": 0,
        "max_attempts": 2,
        "egress_ack": False,
    }
    with db._lock:
        existing = db._conn.execute(
            """SELECT id FROM channel_bulk_jobs
               WHERE inventory_id=? AND directory_alias=? AND state!='cancelled'
               ORDER BY id LIMIT 1""",
            (int(inventory_id), alias),
        ).fetchone()
    if existing is not None:
        job = get_job(db, int(existing[0]))
        job["reused"] = True
        return job
    with db._lock:
        cursor = db._conn.execute(
            """INSERT INTO channel_bulk_jobs(
                chat_id, inventory_id, state, directory_alias, manifest_hash, controls,
                whitelist, generation, cooldown_until, created_at, updated_at
            ) VALUES(?, ?, 'preview', ?, '', ?, ?, 0, 0, ?, ?)""",
            (
                int(inventory["chat_id"]),
                int(inventory_id),
                alias,
                json.dumps(controls, sort_keys=True),
                json.dumps([alias]),
                now,
                now,
            ),
        )
        job_id = int(cursor.lastrowid)
    job = get_job(db, job_id)
    job["reused"] = False
    return job


def update_controls(db, job_id: int, *, now: float, **changes) -> dict:
    job = get_job(db, job_id)
    if job["state"] not in OPEN_STATES:
        raise Refusal("state", "只有预览或暂停中的作业可以改配置")
    controls = dict(job["controls"])
    for key, value in changes.items():
        if key not in controls:
            raise Refusal("config", "未知控制项")
        controls[key] = value
    _validate_controls(controls)
    _save_controls(db, job_id, controls, None, now)
    saved = get_job(db, job_id)
    if saved["controls"] != controls:
        raise Refusal("state", "作业已不在预览或暂停，配置未改")
    return saved


def _validate_controls(controls: dict) -> None:
    if type(controls["min_duration"]) is not int or not 0 <= controls["min_duration"] <= 86400:
        raise Refusal("config", "最短时长无效")
    allow = controls["allow"]
    if (
        not isinstance(allow, list)
        or len(allow) > 500
        or any(type(item) is not int or item <= 0 for item in allow)
    ):
        raise Refusal("config", "复核允许列表无效")
    for key in ("daily_bytes", "monthly_bytes", "down_bps", "up_bps"):
        if type(controls[key]) is not int or controls[key] < 0 or controls[key] > 2**62:
            raise Refusal("config", "额度或速率无效")
    if controls["daily_bytes"] and controls["monthly_bytes"] and controls["daily_bytes"] > controls["monthly_bytes"]:
        raise Refusal("config", "每日额度不能大于每月额度")
    if type(controls["max_attempts"]) is not int or not 1 <= controls["max_attempts"] <= 5:
        raise Refusal("config", "重试次数必须在 1 到 5 之间")
    if type(controls["egress_ack"]) is not bool:
        raise Refusal("config", "需要明确确认远端流量延迟记账")


def set_whitelist(db, job_id: int, aliases: list[str], *, now: float) -> dict:
    job = get_job(db, job_id)
    if job["state"] not in OPEN_STATES:
        raise Refusal("state", "只有预览或暂停中的作业可以改白名单")
    if not aliases or len(aliases) > 32:
        raise Refusal("whitelist", "白名单为空或过长")
    clean = []
    for alias in aliases:
        if not isinstance(alias, str) or not 1 <= len(alias) <= 32 or any(c in "/:%" or ord(c) < 32 for c in alias):
            raise Refusal("whitelist", "目录别名无效")
        if alias not in clean:
            clean.append(alias)
    if job["directory_alias"] not in clean:
        raise Refusal("whitelist", "目标别名必须留在白名单内")
    _save_controls(db, job_id, job["controls"], clean, now)
    return get_job(db, job_id)


def manifest(db, job_id: int, *, now: float, through: int | None = None, expand: bool = False) -> dict:
    """Rebuild the reviewed id list and its hash. Does not start transfers.

    ``through`` is the highest message id the operator has reviewed. Newer ids
    stay out. Once a hash exists, later rebuilds keep that set unless ``expand``
    is explicit, and even then they cannot pass ``through``.
    """
    job = get_job(db, job_id)
    if job["state"] not in OPEN_STATES:
        raise Refusal("state", "运行中的作业不能改清单")
    if through is not None and (type(through) is not int or isinstance(through, bool) or through <= 0):
        raise Refusal("manifest", "审核上界必须是正整数消息编号")
    rows = items(db, job["inventory_id"])
    excluded = exclusions(db, job["chat_id"])
    with db._lock:
        previous = {
            int(row[0])
            for row in db._conn.execute(
                "SELECT message_id FROM channel_bulk_items WHERE job_id=?", (int(job_id),)
            )
        }
    frozen = bool(job["manifest_hash"]) and not expand
    chosen = []
    held_back = 0
    for row in sorted(rows, key=lambda item: int(item["message_id"])):
        message_id = int(row["message_id"])
        if not selected(row, excluded=excluded, dedup=row["dedup"], controls=job["controls"]):
            continue
        if through is not None and message_id > int(through):
            held_back += 1
            continue
        if frozen and message_id not in previous:
            held_back += 1
            continue
        chosen.append(message_id)
    digest = canonical_manifest(
        job_id, job["chat_id"], job["directory_alias"], job["whitelist"], job["controls"], chosen,
        through=through,
    )
    with db._lock:
        db._conn.execute("BEGIN IMMEDIATE")
        try:
            db._conn.execute("DELETE FROM channel_bulk_items WHERE job_id=?", (int(job_id),))
            db._conn.executemany(
                """INSERT INTO channel_bulk_items(job_id, message_id, status, attempts, bytes_charged)
                   VALUES(?, ?, 'pending', 0, 0)""",
                [(int(job_id), message_id) for message_id in chosen],
            )
            cursor = db._conn.execute(
                """UPDATE channel_bulk_jobs SET manifest_hash=?, updated_at=?
                   WHERE id=? AND state IN ('preview','paused')""",
                (digest, now, int(job_id)),
            )
            if cursor.rowcount != 1:
                raise Refusal("state", "作业状态已变化，清单未保存")
            db._conn.execute("COMMIT")
        except BaseException:
            db._conn.execute("ROLLBACK")
            raise
    return {"hash": digest, "count": len(chosen), "ids": chosen, "through": through, "held_back": held_back}


def blockers(db, job_id: int, manifest_hash: str, *, now: float, directories: dict[str, str]) -> list[dict]:
    """Every closed gate, with the fields the operator must supply. No state change."""
    try:
        job = get_job(db, job_id)
    except KeyError:
        return [{"code": "missing", "fields": ["job_id"], "detail": "批量作业不存在"}]
    if job["state"] == "running" and isinstance(manifest_hash, str) and job["manifest_hash"] == manifest_hash.lower():
        return []
    found = []
    if job["state"] not in OPEN_STATES:
        return [{"code": "state", "fields": ["job_id"], "detail": f"状态 {job['state']} 不能恢复"}]
    if (
        not isinstance(manifest_hash, str)
        or len(manifest_hash) != 64
        or job["manifest_hash"] != manifest_hash.lower()
    ):
        found.append({
            "code": "manifest",
            "fields": ["manifest_hash", "through_message_id"],
            "detail": "需要与当前已固定清单一致的 64 位哈希；没有清单时先生成",
        })
    if float(job["cooldown_until"]) > now:
        found.append({"code": "cooldown", "fields": [], "detail": "限流冷却未结束"})
    if job["directory_alias"] not in job["whitelist"] or job["directory_alias"] not in directories:
        found.append({
            "code": "directory",
            "fields": ["alias"],
            "detail": "目标别名不在白名单或未授权目录中",
        })
    controls = job["controls"]
    if controls.get("egress_ack") is not True:
        found.append({
            "code": "egress",
            "fields": ["ack"],
            "detail": "尚未确认网关延迟出站流量也计入本地额度",
        })
    missing_budget = [
        name for name in ("daily_bytes", "monthly_bytes") if int(controls.get(name) or 0) <= 0
    ]
    if missing_budget:
        found.append({
            "code": "budget",
            "fields": missing_budget,
            "detail": "每日和每月字节上限必须是正整数；0 不是无限，程序也不会填供应商额度",
        })
    missing_rate = [name for name in ("down_bps", "up_bps") if int(controls.get(name) or 0) <= 0]
    if missing_rate:
        found.append({
            "code": "rate",
            "fields": missing_rate,
            "detail": "下载和上传字节速率必须是正整数；0 不会关闭限速",
        })
    record = attestation(db)
    if record is None or float(record["period_end"]) <= now:
        found.append({
            "code": "attestation",
            "fields": ["remaining_bytes", "period_end"],
            "detail": "没有未过期的实际剩余额度确认；程序不会默认剩余流量",
        })
    return found


def rejection(db, job_id: int, manifest_hash: str, *, now: float, directories: dict[str, str]) -> str | None:
    """Read-only gate used by the command reply. The worker still rechecks under a lock."""
    try:
        job = get_job(db, job_id)
    except KeyError:
        return "批量作业不存在"
    if job["state"] == "running":
        return None
    if job["state"] not in OPEN_STATES:
        return f"状态 {job['state']} 不能恢复"
    if not isinstance(manifest_hash, str) or job["manifest_hash"] != manifest_hash.lower():
        return "清单已变化，请重新生成并核对后再恢复"
    if float(job["cooldown_until"]) > now:
        return "限流冷却未结束"
    if job["directory_alias"] not in job["whitelist"] or job["directory_alias"] not in directories:
        return "目标别名不在白名单或未授权目录中"
    controls = job["controls"]
    if not controls.get("egress_ack"):
        return "尚未确认网关延迟出站流量也计入本地额度"
    if int(controls.get("daily_bytes") or 0) <= 0 or int(controls.get("monthly_bytes") or 0) <= 0:
        return "必须填写每日和每月字节上限，程序不会假定供应商额度"
    if int(controls.get("down_bps") or 0) <= 0 or int(controls.get("up_bps") or 0) <= 0:
        return "必须填写下载和上传字节速率上限"
    record = attestation(db)
    if record is None or float(record["period_end"]) <= now:
        return "没有未过期的实际剩余额度确认"
    return None


def prepare_resume(
    db,
    job_id: int,
    manifest_hash: str,
    *,
    now: float,
    disk_free: int,
    min_free: int,
    directories: dict[str, str],
) -> dict:
    """Move preview/paused to running only when every gate passes. No I/O."""
    if not isinstance(manifest_hash, str) or len(manifest_hash) != 64:
        raise Refusal("manifest", "清单校验值无效")
    ensure_db(db)
    with db._lock:
        db._conn.execute("BEGIN IMMEDIATE")
        try:
            row = db._conn.execute(
                "SELECT * FROM channel_bulk_jobs WHERE id=?", (int(job_id),)
            ).fetchone()
            if row is None:
                raise Refusal("missing", "批量作业不存在")
            job = dict(row)
            if job["state"] == "running":
                db._conn.execute("COMMIT")
                return {"ok": True, "idempotent": True, "job_id": int(job_id)}
            if job["state"] not in OPEN_STATES:
                raise Refusal("state", f"状态 {job['state']} 不能恢复")
            if job["manifest_hash"] != manifest_hash.lower():
                raise Refusal("manifest", "清单已变化，请重新生成并核对后再恢复")
            if float(job["cooldown_until"]) > now:
                raise Refusal("cooldown", "限流冷却未结束")
            controls = json.loads(job["controls"])
            whitelist = json.loads(job["whitelist"])
            alias = job["directory_alias"]
            if alias not in whitelist or alias not in directories:
                raise Refusal("directory", "目标别名不在白名单或未授权目录中")
            if not controls.get("egress_ack"):
                raise Refusal("egress", "尚未确认网关延迟出站流量也计入本地额度")
            if int(controls.get("daily_bytes") or 0) <= 0 or int(controls.get("monthly_bytes") or 0) <= 0:
                raise Refusal("budget", "必须填写每日和每月字节上限，程序不会假定供应商额度")
            if int(controls.get("down_bps") or 0) <= 0 or int(controls.get("up_bps") or 0) <= 0:
                raise Refusal("rate", "必须填写下载和上传字节速率上限")
            record = db._conn.execute(
                "SELECT * FROM channel_quota_attestation WHERE id=1"
            ).fetchone()
            if record is None or float(record["period_end"]) <= now:
                raise Refusal("attestation", "没有未过期的实际剩余额度确认")
            pending = db._conn.execute(
                """SELECT message_id FROM channel_bulk_items
                   WHERE job_id=? AND status='pending' ORDER BY message_id LIMIT 1""",
                (int(job_id),),
            ).fetchone()
            cursor = db._conn.execute(
                """UPDATE channel_bulk_jobs SET state='running', generation=generation+1,
                   error=NULL, updated_at=? WHERE id=? AND generation=? AND state IN ('preview','paused')""",
                (now, int(job_id), int(job["generation"])),
            )
            if cursor.rowcount != 1:
                current = db._conn.execute(
                    "SELECT state FROM channel_bulk_jobs WHERE id=?", (int(job_id),)
                ).fetchone()
                if current and current["state"] == "running":
                    db._conn.execute("COMMIT")
                    return {"ok": True, "idempotent": True, "job_id": int(job_id)}
                raise Refusal("race", "恢复请求冲突，未启动")
            db._conn.execute("COMMIT")
            next_id = int(pending[0]) if pending else None
        except BaseException:
            if db._conn.in_transaction:
                db._conn.execute("ROLLBACK")
            raise
    remaining = attested_remaining(db, now)
    if next_id is not None and (remaining is None or remaining < 0):
        pause(db, job_id, now=now, error="额度确认已不足以继续")
        raise Refusal("attestation", "额度确认已不足以继续")
    return {
        "ok": True,
        "idempotent": False,
        "job_id": int(job_id),
        "directory": directories[alias],
        "next_message": next_id,
        "disk_free": int(disk_free),
        "min_free": int(min_free),
    }


def pause(db, job_id: int, *, now: float, error: str | None = None) -> dict:
    with db._lock:
        db._conn.execute(
            """UPDATE channel_bulk_jobs SET state='paused', error=?, updated_at=?
               WHERE id=? AND state IN ('preview','paused','running')""",
            (error, now, int(job_id)),
        )
    return get_job(db, job_id)


def cancel(db, job_id: int, *, now: float) -> dict:
    """Stop scheduling further items. Does not cancel unrelated tasks."""
    with db._lock:
        db._conn.execute(
            """UPDATE channel_bulk_jobs SET state='cancelled', error='cancelled by admin', updated_at=?
               WHERE id=? AND state!='done'""",
            (now, int(job_id)),
        )
    return get_job(db, job_id)


def hold_on_startup(db, now: float | None = None) -> dict[str, int]:
    """Process start never resumes inventory or bulk work."""
    import time

    moment = time.time() if now is None else now
    from tg2cloud.channel_bulk.inventory import hold_running

    inventories = hold_running(db, moment)
    ensure_db(db)
    with db._lock:
        cursor = db._conn.execute(
            """UPDATE channel_bulk_jobs SET state='paused',
               error='restart; explicit resume required', updated_at=?
               WHERE state='running'""",
            (moment,),
        )
        jobs = int(cursor.rowcount)
    return {"inventories": inventories, "jobs": jobs}


def candidate_rows(db, job: dict) -> list[dict]:
    excluded = exclusions(db, job["chat_id"])
    rows = []
    for row in items(db, job["inventory_id"]):
        kind, task = classify(
            db,
            chat_id=job["chat_id"],
            message_id=int(row["message_id"]),
            media_key=row.get("media_key"),
            file_unique_id=row.get("file_unique_id"),
        )
        row = dict(row)
        row["dedup"] = kind
        row["blocked_task"] = None if task is None else int(task["id"])
        row["chosen"] = selected(row, excluded=excluded, dedup=kind, controls=job["controls"])
        rows.append(row)
    return rows


def attestation_view(db, now: float) -> dict | None:
    record = attestation(db)
    if record is None:
        return None
    record = dict(record)
    record["effective_remaining"] = attested_remaining(db, now)
    return record
