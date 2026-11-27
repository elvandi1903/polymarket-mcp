"""Conservative UTC BUY reservations. No order submission is performed here."""
from __future__ import annotations

import os
import sqlite3
import stat
import time
from contextlib import closing
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder

from . import observability, safety

_COLUMNS = {
    "slug": "TEXT NOT NULL DEFAULT ''",
    "outcome": "TEXT NOT NULL DEFAULT ''",
    "max_price": "TEXT NOT NULL DEFAULT ''",
    "notional": "TEXT NOT NULL DEFAULT ''",
    "submitted": "INTEGER NOT NULL DEFAULT 0",
    "created_at": "REAL NOT NULL DEFAULT 0",
}


class BuyLedger:
    def __init__(self, path: Path) -> None:
        self.path = path.absolute()

    # -- private-file discipline ------------------------------------------
    def _check_file(self, *, create: bool = False) -> None:
        if create:
            observability._private_dir(self.path.parent)
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        if create:
            flags |= os.O_CREAT
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise PermissionError("BUY ledger file must be private")
        finally:
            os.close(fd)

    def _connect(self, *, create: bool = False) -> sqlite3.Connection:
        self._check_file(create=create)
        db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=10)
        db.execute("CREATE TABLE IF NOT EXISTS claims (round INTEGER PRIMARY KEY, "
                   "day INTEGER NOT NULL, amount TEXT NOT NULL, status TEXT NOT NULL)")
        existing = {row[1] for row in db.execute("PRAGMA table_info(claims)")}
        for name, decl in _COLUMNS.items():
            if name not in existing:
                try:
                    db.execute(f"ALTER TABLE claims ADD COLUMN {name} {decl}")
                except sqlite3.OperationalError as exc:
                    # A concurrent writer may have added it between PRAGMA and ALTER.
                    if "duplicate column" not in str(exc):
                        raise
        db.commit()
        return db

    # -- writes -----------------------------------------------------------
    def reserve(self, window_start: int, amount: Decimal, daily_cap: Decimal, *,
                now: int, slug: str = "", outcome: str = "", max_price: str = "",
                notional: str = "") -> None:
        if not daily_cap.is_finite() or daily_cap <= 0:
            raise safety.ValidationError("POLY_MAX_DAILY_NOTIONAL must be positive")
        if not amount.is_finite() or not 0 < amount <= safety.MAX_BUY_SPEND:
            raise safety.ValidationError("BUY reservation must be positive and at most USD 5")
        if window_start <= 0 or window_start != now // 300 * 300:
            raise safety.ValidationError("BUY reservation requires the current round")
        with closing(self._connect(create=True)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM claims WHERE round = ?", (window_start,)).fetchone():
                raise safety.ValidationError("BUY round already claimed")
            day = now // 86_400
            active = db.execute("SELECT amount FROM claims WHERE day = ? AND status != 'no_fill'",
                                (day,))
            used = sum(Fraction(value) for (value,) in active)
            if used + Fraction(amount) > Fraction(daily_cap):
                raise safety.ValidationError("BUY daily budget exceeded")
            db.execute(
                "INSERT INTO claims (round, day, amount, status, slug, outcome, max_price, "
                "notional, submitted, created_at) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, 0, ?)",
                (window_start, day, str(amount), slug, outcome, max_price, notional, float(now)),
            )

    def mark_submitted(self, window_start: int) -> None:
        with closing(self._connect()) as db, db:
            cursor = db.execute(
                "UPDATE claims SET submitted = 1 WHERE round = ? AND status = 'pending'",
                (window_start,))
            if cursor.rowcount != 1:
                raise safety.ValidationError("BUY claim missing or already finalized")

    def abort(self, window_start: int) -> None:
        """Release a claim only if nothing could have reached the venue."""
        with closing(self._connect()) as db, db:
            db.execute("DELETE FROM claims WHERE round = ? AND status = 'pending' AND submitted = 0",
                       (window_start,))

    def finish(self, window_start: int, response: AcceptedOrder | RejectedOrder | None) -> None:
        # SDK 0.20.1 does not expose INVALID as a typed rejection and does not
        # document whether an accepted FAK response was fully or partially
        # filled, so an accepted order stays "unknown" until the operator
        # reconciles it. Only an explicit no-fill rejection releases budget.
        status = "unknown"
        if isinstance(response, RejectedOrder) and response.code in ("unmatched", "not_enough_balance"):
            status = "no_fill"
        with closing(self._connect()) as db, db:
            cursor = db.execute(
                "UPDATE claims SET status = ? WHERE round = ? AND status = 'pending'",
                (status, window_start))
            if cursor.rowcount != 1:
                raise safety.ValidationError("BUY claim missing or already finalized")

    def resolve(self, window_start: int, status: str) -> None:
        """Human CLI only: operator confirms the venue outcome."""
        if status not in ("matched", "no_fill"):
            raise safety.ValidationError("status must be 'matched' or 'no_fill'")
        with closing(self._connect()) as db, db:
            cursor = db.execute(
                "UPDATE claims SET status = ? WHERE round = ? AND status IN ('pending', 'unknown')",
                (status, window_start))
            if cursor.rowcount != 1:
                raise safety.ValidationError("BUY claim missing or already finalized")

    # -- reads ------------------------------------------------------------
    def claim(self, window_start: int) -> str | None:
        try:
            self._check_file()
        except FileNotFoundError:
            return None
        with closing(self._connect()) as db:
            row = db.execute("SELECT status FROM claims WHERE round = ?", (window_start,)).fetchone()
        if row is None:
            return None
        return "unknown" if row[0] == "pending" else row[0]

    def unreconciled(self, limit: int = 12) -> list[int]:
        try:
            self._check_file()
        except FileNotFoundError:
            return []
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT round FROM claims WHERE status IN ('pending', 'unknown') "
                "ORDER BY round DESC LIMIT ?", (limit,)).fetchall()
        return [row[0] for row in rows]

    def rows_for_day(self, day: int) -> list[dict]:
        try:
            self._check_file()
        except FileNotFoundError:
            return []
        with closing(self._connect()) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM claims WHERE day = ? ORDER BY round", (day,)).fetchall()
        return [dict(row) for row in rows]

    def all_rows(self) -> list[dict]:
        try:
            self._check_file()
        except FileNotFoundError:
            return []
        with closing(self._connect()) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM claims ORDER BY round").fetchall()
        return [dict(row) for row in rows]


def day_of(now: float) -> int:
    return int(now) // 86_400


def loss_report(rows: list[dict], resolutions: dict[int, str | None], *,
                max_daily_loss: Decimal, max_consecutive_losses: int) -> dict:
    """Conservative PnL: a loss is charged in full, a win is never credited.

    ``resolutions`` maps window_start -> winning side ("UP"/"DOWN"/None).
    Rows without a known result are skipped, not guessed.
    """
    losses = Decimal(0)
    streak = 0
    counted = 0
    for row in sorted(rows, key=lambda r: r["round"], reverse=True):
        winner = resolutions.get(row["round"])
        if row["status"] not in ("matched", "no_fill") or winner is None:
            continue
        counted += 1
        if row["status"] == "no_fill":
            streak = 0
            continue
        if row.get("outcome", "").upper() == winner.upper():
            streak = 0
            continue
        amount = Decimal(str(row.get("notional") or row["amount"]))
        losses += amount
        streak += 1
    reasons = []
    if losses > max_daily_loss:
        reasons.append("daily_loss_limit")
    if streak >= max_consecutive_losses:
        reasons.append("consecutive_loss_limit")
    return {
        "resolved_claims": counted, "daily_loss_usd": str(losses), "estimate": True,
        "consecutive_losses": streak, "reasons": reasons,
    }


def now_seconds() -> float:
    return time.time()
