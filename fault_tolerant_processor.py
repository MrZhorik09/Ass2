from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Transaction:
    id: str
    amount: int
    failure_type: str


TRANSACTIONS = [
    Transaction("T001", 12000, "None"),
    Transaction("T002", 25000, "Network"),
    Transaction("T003", 8000, "None"),
    Transaction("T004", 45000, "Timeout"),
    Transaction("T005", 13000, "None"),
    Transaction("T006", 70000, "Database"),
    Transaction("T007", 9000, "None"),
    Transaction("T008", 31000, "Network"),
    Transaction("T009", 15000, "None"),
    Transaction("T010", 50000, "Timeout"),
    Transaction("T011", 6000, "None"),
    Transaction("T012", 80000, "Database"),
    Transaction("T013", 11000, "None"),
    Transaction("T014", 22000, "None"),
    Transaction("T015", 40000, "Network"),
]


FAULT_RULES = {
    "Network":  [True, False],
    "Timeout":  [True, True, False],
    "Database": [True, True, True],
}
MAX_RETRIES = 2
CHECKPOINT_EVERY = 5
BACKOFF_BASE_S = 0.01


FAULT_STAGE = {"Network": "Process", "Timeout": "Process", "Database": "Record"}

ATTEMPT_LABEL = {0: "Initial", 1: "Retry 1", 2: "Retry 2"}
OUT = Path("output")


class TransactionFault(Exception):
    failure_type = "Unknown"
    retryable = False

    def __init__(self, txn_id: str, stage: str, attempt: int):
        self.txn_id, self.stage, self.attempt = txn_id, stage, attempt
        super().__init__(f"{self.failure_type} fault during {stage} "
                         f"for {txn_id} ({ATTEMPT_LABEL.get(attempt, attempt)})")


class NetworkFault(TransactionFault):
    failure_type, retryable = "Network", True


class TimeoutFault(TransactionFault):
    failure_type, retryable = "Timeout", True


class DatabaseFault(TransactionFault):

    failure_type, retryable = "Database", True


class ValidationError(Exception):
    pass


FAULT_CLASSES = {"Network": NetworkFault, "Timeout": TimeoutFault, "Database": DatabaseFault}


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        entry.update(getattr(record, "ctx", {}))
        return json.dumps(entry, ensure_ascii=False)


def build_logger() -> logging.Logger:
    OUT.mkdir(exist_ok=True)
    logger = logging.getLogger("payments")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(OUT / "run_log.jsonl", mode="w", encoding="utf-8")
    fh.setFormatter(JsonLineFormatter())
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(levelname)-7s %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


LOG = logging.getLogger("payments")


def log(level: int, msg: str, **ctx) -> None:
    LOG.log(level, msg, extra={"ctx": ctx})


@dataclass
class Ledger:
    recorded: dict = field(default_factory=dict)
    duplicate_writes_suppressed: int = 0

    def record(self, txn_id: str, amount: int) -> bool:
        if txn_id in self.recorded:
            self.duplicate_writes_suppressed += 1
            return False
        self.recorded[txn_id] = amount
        return True

    def discard(self, txn_id: str) -> None:
        self.recorded.pop(txn_id, None)

    @property
    def count(self) -> int:
        return len(self.recorded)

    @property
    def total(self) -> int:
        return sum(self.recorded.values())


class PaymentService:
    def __init__(self) -> None:
        self.ledger = Ledger()

    @staticmethod
    def _inject(txn: Transaction, stage: str, attempt: int) -> None:
        ft = txn.failure_type
        if ft == "None" or FAULT_STAGE[ft] != stage:
            return
        script = FAULT_RULES[ft]
        if attempt < len(script) and script[attempt]:
            raise FAULT_CLASSES[ft](txn.id, stage, attempt)

    @staticmethod
    def validate(txn: Transaction) -> None:
        if txn.amount <= 0:
            raise ValidationError(f"{txn.id}: amount must be positive")
        if not txn.id.startswith("T"):
            raise ValidationError(f"{txn.id}: malformed id")

    def execute(self, txn: Transaction, attempt: int) -> bool:
        self.validate(txn)
        self._inject(txn, "Validate", attempt)
        self._inject(txn, "Process", attempt)
        written = self.ledger.record(txn.id, txn.amount)
        if not written:
            log(logging.WARNING, f"{txn.id}: duplicate ledger write suppressed (idempotency key)",
                txn=txn.id, event="idempotent_skip", attempt=attempt)
        self._inject(txn, "Record", attempt)
        return written


class CheckpointManager:

    def __init__(self, directory: Path):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        for old in self.dir.glob("*.json"):
            old.unlink()
        self.checkpoints: list[dict] = []
        self.journal: list[tuple[str, int]] = []
        self._base = {"label": "CP0", "recorded": {}, "total": 0, "count": 0}

    def latest(self) -> dict:
        return self.checkpoints[-1] if self.checkpoints else self._base

    def commit(self, txn: Transaction) -> None:
        self.journal.append((txn.id, txn.amount))

    def create(self, ledger: Ledger, last_txn: str, kind: str = "periodic") -> dict:
        label = f"CP{len(self.checkpoints) + 1}"
        cp = {
            "label": label,
            "kind": kind,
            "created_at": datetime.now().isoformat(timespec="milliseconds"),
            "count": ledger.count,
            "total": ledger.total,
            "last_txn": last_txn,
            "recorded": copy.deepcopy(ledger.recorded),
        }
        path = self.dir / f"{label}.json"
        path.write_text(json.dumps(cp, indent=2), encoding="utf-8")
        cp["file"] = path.as_posix()
        self.checkpoints.append(cp)
        self.journal.clear()
        log(logging.INFO, f"{label} created ({kind}): {cp['count']} txns, {cp['total']} KZT",
            event="checkpoint", checkpoint=label, count=cp["count"], total=cp["total"],
            last_txn=last_txn, file=cp["file"])
        return cp

    def rollback(self, ledger: Ledger, failed_txn: str) -> str:
        cp = self.latest()
        dirty = {k: v for k, v in ledger.recorded.items()
                 if k not in cp["recorded"] and k not in dict(self.journal)}
        ledger.recorded = copy.deepcopy(cp["recorded"])
        for txn_id, amount in self.journal:
            ledger.record(txn_id, amount)
        log(logging.ERROR, f"{failed_txn}: ROLLBACK to {cp['label']} "
            f"(discarded dirty rows {list(dirty)}, replayed {len(self.journal)} journal rows)",
            txn=failed_txn, event="rollback", checkpoint=cp["label"],
            discarded=list(dirty), replayed=len(self.journal),
            state_count=ledger.count, state_total=ledger.total)
        return cp["label"]


def run_part_a() -> dict:
    log(logging.INFO, "=== PART A: baseline (no fault tolerance) ===", part="A")
    svc = PaymentService()
    attempted, crash = [], None
    try:
        for txn in TRANSACTIONS:
            attempted.append(txn.id)
            svc.execute(txn, attempt=0)
            log(logging.INFO, f"{txn.id}: SUCCESS", part="A", txn=txn.id, status="SUCCESS")
    except TransactionFault as exc:
        crash = str(exc)
        log(logging.CRITICAL, f"Processor STOPPED: {exc}", part="A", txn=exc.txn_id,
            status="CRASH", failure=exc.failure_type)

    total = sum(t.amount for t in TRANSACTIONS)
    res = {
        "attempted": len(attempted),
        "successful": svc.ledger.count,
        "lost": len(TRANSACTIONS) - svc.ledger.count,
        "processed_amount": svc.ledger.total,
        "lost_amount": total - svc.ledger.total,
        "stopped_at": attempted[-1] if crash else None,
        "crash": crash,
    }
    log(logging.INFO, "Part A metrics", part="A", **res)
    return res


def run_part_b() -> dict:
    log(logging.INFO, "=== PART B: exception handling (classify, log, continue) ===", part="B")
    svc = PaymentService()
    rows, confirmed = [], {}
    for txn in TRANSACTIONS:
        try:
            svc.execute(txn, attempt=0)
            confirmed[txn.id] = txn.amount
            log(logging.INFO, f"{txn.id}: SUCCESS", part="B", txn=txn.id, status="SUCCESS")
        except TransactionFault as exc:
            row = {
                "transaction": txn.id,
                "failure": exc.failure_type,
                "exception": type(exc).__name__,
                "stage": exc.stage,
                "recovery_action": "Caught, classified, logged; transaction skipped; continue",
                "final_status": "FAILED",
            }
            rows.append(row)
            log(logging.ERROR, f"{txn.id}: {exc} -> FAILED, continuing", part="B",
                txn=txn.id, failure=exc.failure_type, exception=type(exc).__name__,
                stage=exc.stage, status="FAILED")
            continue
        except ValidationError as exc:
            log(logging.ERROR, f"{txn.id}: validation error {exc}", part="B", txn=txn.id)

    with open(OUT / "partB_exceptions.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    res = {
        "rows": rows,
        "successful": len(confirmed),
        "failed": len(rows),
        "confirmed_amount": sum(confirmed.values()),

        "ledger_total_without_rollback": svc.ledger.total,
        "dirty_rows": sorted(set(svc.ledger.recorded) - set(confirmed)),
    }
    log(logging.INFO, "Part B metrics", part="B",
        **{k: v for k, v in res.items() if k != "rows"})
    return res


def run_fault_tolerant(part: str, use_checkpoints: bool) -> dict:
    title = "retry mechanism" if part == "C" else "retry + checkpoint + rollback"
    log(logging.INFO, f"=== PART {part}: {title} ===", part=part)
    svc = PaymentService()
    cpm = CheckpointManager(OUT / "checkpoints") if use_checkpoints else None
    attempt_log, statuses = [], {}
    step = retries = rollbacks = 0
    since_cp = 0

    def add(txn, attempt, failure, action, result):
        nonlocal step
        step += 1
        row = {
            "step": step,
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "transaction": txn.id,
            "attempt": ATTEMPT_LABEL[attempt],
            "failure": failure,
            "action": action,
            "result": result,
        }
        attempt_log.append(row)
        log(logging.INFO if result == "SUCCESS" else logging.WARNING,
            f"[{step:02d}] {txn.id} {row['attempt']:<8} {failure:<9} {action} -> {result}",
            part=part, event="attempt", **row)

    for txn in TRANSACTIONS:
        for attempt in range(MAX_RETRIES + 1):
            try:
                svc.execute(txn, attempt)
                add(txn, attempt, "None", "Validate->Process->Record committed", "SUCCESS")
                statuses[txn.id] = "SUCCESS"
                if cpm:
                    cpm.commit(txn)
                    since_cp += 1
                    if since_cp == CHECKPOINT_EVERY:
                        cpm.create(svc.ledger, last_txn=txn.id)
                        since_cp = 0
                break
            except TransactionFault as exc:
                if exc.retryable and attempt < MAX_RETRIES:
                    delay = BACKOFF_BASE_S * 2 ** attempt
                    add(txn, attempt, exc.failure_type,
                        f"{type(exc).__name__} in {exc.stage}; backoff {delay:.2f}s, retry", "FAIL")
                    retries += 1
                    time.sleep(delay)
                    continue

                if cpm:
                    label = cpm.rollback(svc.ledger, txn.id)
                    action = f"Retries exhausted; ROLLBACK to {label}"
                else:
                    svc.ledger.discard(txn.id)
                    action = "Retries exhausted; rollback (undo own ledger write)"
                add(txn, attempt, exc.failure_type, action, "ROLLBACK")
                rollbacks += 1
                statuses[txn.id] = "ROLLED_BACK"
                break

    if cpm and since_cp:
        cpm.create(svc.ledger, last_txn=TRANSACTIONS[-1].id, kind="final (end of batch)")

    fname = f"attempt_log_part{part}.csv"
    with open(OUT / fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(attempt_log[0].keys()))
        w.writeheader()
        w.writerows(attempt_log)

    total = sum(t.amount for t in TRANSACTIONS)
    res = {
        "successful": svc.ledger.count,
        "failed": sum(1 for s in statuses.values() if s != "SUCCESS"),
        "processed_amount": svc.ledger.total,
        "lost_amount": total - svc.ledger.total,
        "retries": retries,
        "rollbacks": rollbacks,
        "duplicate_writes_suppressed": svc.ledger.duplicate_writes_suppressed,
        "attempts": len(attempt_log),
        "statuses": statuses,
        "attempt_log": attempt_log,
        "checkpoints": [{k: v for k, v in cp.items() if k != "recorded"} | {"txns": list(cp["recorded"])}
                        for cp in (cpm.checkpoints if cpm else [])],
    }
    log(logging.INFO, f"Part {part} metrics", part=part,
        **{k: v for k, v in res.items() if k not in ("attempt_log", "statuses", "checkpoints")})
    return res


def pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def run_part_e(a: dict, d: dict) -> dict:
    n = len(TRANSACTIONS)
    base_failed = a["lost"]
    ft_failed = d["failed"]
    e = {
        "baseline": {"successful": a["successful"], "failed": base_failed,
                     "lost_amount": a["lost_amount"], "retries": 0, "rollbacks": 0,
                     "completion_rate": a["successful"] / n},
        "fault_tolerant": {"successful": d["successful"], "failed": ft_failed,
                           "lost_amount": d["lost_amount"], "retries": d["retries"],
                           "rollbacks": d["rollbacks"], "completion_rate": d["successful"] / n},
    }
    e["recovery_improvement_pp"] = e["fault_tolerant"]["completion_rate"] - e["baseline"]["completion_rate"]
    e["txn_loss_reduction"] = (base_failed - ft_failed) / base_failed
    e["amount_loss_reduction"] = (a["lost_amount"] - d["lost_amount"]) / a["lost_amount"]
    faulted = [t for t in TRANSACTIONS if t.failure_type != "None"]
    recovered_by_retry = sum(1 for t in faulted if d["statuses"][t.id] == "SUCCESS")
    e["faults_total"] = len(faulted)
    e["recovered_by_retry"] = recovered_by_retry
    e["retry_recovery_rate"] = recovered_by_retry / len(faulted)
    log(logging.INFO, "Part E analysis", part="E",
        recovery_improvement=pct(e["recovery_improvement_pp"]),
        txn_loss_reduction=pct(e["txn_loss_reduction"]),
        amount_loss_reduction=pct(e["amount_loss_reduction"]))
    return e


def md_table(headers, rows) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def write_tables(meta, a, b, c, d, e) -> None:
    kzt = lambda v: f"{v:,}".replace(",", " ")
    parts = [f"# Generated results\n\nRun: {meta['run_at']} | source: {meta['source']} | "
             f"commit: {meta['commit']} | Python {meta['python']}\n"]
    parts.append("## Part A - Baseline\n" + md_table(["Metric", "Result"], [
        ["Transactions attempted", a["attempted"]],
        ["Successful transactions", a["successful"]],
        ["Transactions lost", a["lost"]],
        ["Processed amount (KZT)", kzt(a["processed_amount"])],
        ["Lost amount (KZT)", kzt(a["lost_amount"])],
    ]) + f"\n\nStopped at {a['stopped_at']}: `{a['crash']}`\n")
    parts.append("## Part B - Exception handling\n" + md_table(
        ["Transaction", "Failure", "Exception", "Recovery Action", "Final Status"],
        [[r["transaction"], r["failure"], f"{r['exception']} ({r['stage']})",
          r["recovery_action"], r["final_status"]] for r in b["rows"]]) +
        f"\n\nSuccessful {b['successful']}, failed {b['failed']}, confirmed amount "
        f"{kzt(b['confirmed_amount'])} KZT. Ledger total without rollback: "
        f"{kzt(b['ledger_total_without_rollback'])} KZT (dirty rows: {', '.join(b['dirty_rows'])}).\n")
    parts.append("## Part C - Retry attempt log\n" + md_table(
        ["Step", "Timestamp", "Transaction", "Attempt", "Failure", "Action", "Result"],
        [[r["step"], r["timestamp"], r["transaction"], r["attempt"], r["failure"],
          r["action"], r["result"]] for r in c["attempt_log"]]) +
        f"\n\nRetries: {c['retries']}, rollbacks: {c['rollbacks']}, "
        f"duplicate writes suppressed: {c['duplicate_writes_suppressed']}\n")
    parts.append("## Part D - Checkpoints\n" + md_table(
        ["Checkpoint", "Successful transactions", "Total amount (KZT)", "State saved"],
        [[cp["label"] + ("" if cp["kind"] == "periodic" else f" ({cp['kind']})"),
          cp["count"], kzt(cp["total"]),
          f"{cp['file']}: {cp['txns'][0]}…{cp['txns'][-1]}, last={cp['last_txn']}"]
         for cp in d["checkpoints"]]) + "\n")
    B, F = e["baseline"], e["fault_tolerant"]
    parts.append("## Part E - Before/after\n" + md_table(
        ["Metric", "Baseline", "Fault-tolerant", "Improvement"], [
            ["Successful transactions", B["successful"], F["successful"], f"+{F['successful'] - B['successful']}"],
            ["Failed transactions", B["failed"], F["failed"], f"-{B['failed'] - F['failed']}"],
            ["Lost amount (KZT)", kzt(B["lost_amount"]), kzt(F["lost_amount"]),
             f"-{kzt(B['lost_amount'] - F['lost_amount'])} ({pct(e['amount_loss_reduction'])})"],
            ["Retries", B["retries"], F["retries"], f"{F['retries']} faults absorbed by retry attempts"],
            ["Rollbacks", B["rollbacks"], F["rollbacks"], "ledger kept consistent"],
            ["Completion rate", pct(B["completion_rate"]), pct(F["completion_rate"]),
             f"+{e['recovery_improvement_pp'] * 100:.2f} pp"],
        ]) +
        f"\n\nRecovery improvement = {pct(F['completion_rate'])} - {pct(B['completion_rate'])} = "
        f"+{e['recovery_improvement_pp'] * 100:.2f} pp\n\n"
        f"Transaction-loss reduction = ({B['failed']} - {F['failed']}) / {B['failed']} = "
        f"{pct(e['txn_loss_reduction'])}\n\n"
        f"Retry recovered {e['recovered_by_retry']} of {e['faults_total']} faulted transactions "
        f"({pct(e['retry_recovery_rate'])}).\n")
    (OUT / "tables.md").write_text("\n".join(parts), encoding="utf-8")


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return "not-a-git-repo"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--part", choices=list("ABCDE") + ["all"], default="all")
    args = ap.parse_args()

    build_logger()
    meta = {"run_at": datetime.now().isoformat(timespec="seconds"),
            "source": Path(__file__).name, "commit": git_commit(),
            "python": sys.version.split()[0]}
    log(logging.INFO, "Run started", **meta)

    p = args.part
    a = run_part_a() if p in ("A", "E", "all") else None
    b = run_part_b() if p in ("B", "all") else None
    c = run_fault_tolerant("C", use_checkpoints=False) if p in ("C", "all") else None
    d = run_fault_tolerant("D", use_checkpoints=True) if p in ("D", "E", "all") else None
    e = run_part_e(a, d) if a and d else None

    if p == "all":
        write_tables(meta, a, b, c, d, e)
        strip = lambda r: {k: v for k, v in r.items() if k != "attempt_log"}
        (OUT / "results.json").write_text(json.dumps(
            {"meta": meta, "A": a, "B": b, "C": strip(c), "D": strip(d), "E": e},
            indent=2, ensure_ascii=False), encoding="utf-8")
        log(logging.INFO, f"Outputs written to {OUT.resolve()}")


if __name__ == "__main__":
    main()
