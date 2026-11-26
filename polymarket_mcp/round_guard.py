"""Conservative UTC BUY reservations. No order submission is performed here."""
from __future__ import annotations

import os
import sqlite3
import stat
from contextlib import closing
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder

from . import observability, safety


class BuyLedger:
    def __init__(self, path: Path) -> None:
        self.path = path.absolute()

    def _check_file(self, *, create: bool = False) -> None:
        if create:
            observability._private_dir(self.path.parent)
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        if create:
            flags |= os.O_CREAT
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_nlink != 1:
                raise PermissionError("BUY ledger file must be private")
        finally:
            os.close(fd)

    def reserve(self, window_start: int, amount: Decimal, daily_cap: Decimal, *, now: int) -> None:
        if not daily_cap.is_finite() or daily_cap <= 0:
            raise safety.ValidationError("POLY_MAX_DAILY_NOTIONAL must be positive")
        if not amount.is_finite() or not 0 < amount <= safety.MAX_BUY_SPEND:
            raise safety.ValidationError("BUY reservation must be positive and at most USD 5")
        if window_start <= 0 or window_start != now // 300 * 300:
            raise safety.ValidationError("BUY reservation requires the current round")
        self._check_file(create=True)
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS claims (round INTEGER PRIMARY KEY, "
                       "day INTEGER NOT NULL, amount TEXT NOT NULL, status TEXT NOT NULL)")
            if db.execute("SELECT 1 FROM claims WHERE round = ?", (window_start,)).fetchone():
                raise safety.ValidationError("BUY round already claimed")
            day = now // 86_400
            active = db.execute("SELECT amount FROM claims WHERE day = ? AND status != 'no_fill'", (day,))
            used = sum(Fraction(value) for (value,) in active)
            if used + Fraction(amount) > Fraction(daily_cap):
                raise safety.ValidationError("BUY daily budget exceeded")
            db.execute("INSERT INTO claims VALUES (?, ?, ?, 'pending')", (window_start, day, str(amount)))

    def finish(self, window_start: int, response: AcceptedOrder | RejectedOrder | None) -> None:
        # SDK 0.20.1 does not expose INVALID as a typed rejection. An
        # unrecognized response stays charged rather than guessing from text.
        status = "unknown"
        if isinstance(response, RejectedOrder) and response.code in ("unmatched", "not_enough_balance"):
            status = "no_fill"
        elif isinstance(response, AcceptedOrder) and response.status == "matched":
            status = "matched"
        self._check_file()
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True)) as db, db:
            cursor = db.execute("UPDATE claims SET status = ? WHERE round = ? AND status = 'pending'",
                                (status, window_start))
            if cursor.rowcount != 1:
                raise safety.ValidationError("BUY claim missing or already finalized")

    def claim(self, window_start: int) -> str | None:
        try:
            self._check_file()
        except FileNotFoundError:
            return None
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            row = db.execute("SELECT status FROM claims WHERE round = ?", (window_start,)).fetchone()
        if row is None:
            return None
        return "unknown" if row[0] == "pending" else row[0]
