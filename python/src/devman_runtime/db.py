from __future__ import annotations

import sqlite3
from pathlib import Path


class OwnershipDB:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ownership (
                resource TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS link_groups (
                client TEXT NOT NULL,
                group_idx INTEGER NOT NULL,
                resource TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (client, group_idx, resource)
            )
            """
        )
        self._conn.commit()

    def owner_of(self, resource: str) -> str | None:
        row = self._conn.execute(
            "SELECT owner FROM ownership WHERE resource = ?", (resource,)
        ).fetchone()
        if row is None:
            return None
        return str(row[0])

    def acquire(self, resource: str, owner: str) -> bool:
        current = self.owner_of(resource)
        if current is None:
            self._conn.execute(
                "INSERT INTO ownership(resource, owner) VALUES(?, ?)", (resource, owner)
            )
            self._conn.commit()
            return True
        if current == owner:
            return True
        return False

    def release(self, resource: str, owner: str) -> bool:
        current = self.owner_of(resource)
        if current != owner:
            return False
        self._conn.execute("DELETE FROM ownership WHERE resource = ?", (resource,))
        self._conn.commit()
        return True

    def release_all_by_owner(self, owner: str) -> int:
        cur = self._conn.execute("DELETE FROM ownership WHERE owner = ?", (owner,))
        self._conn.commit()
        return int(cur.rowcount)

    def set_link_groups(self, client: str, groups: list[list[str]]) -> int:
        """Replace all link groups registered by a client.

        Groups persist across client disconnects so a server-side watchdog
        can keep protecting them while the client application is closed.
        """
        self._conn.execute("DELETE FROM link_groups WHERE client = ?", (client,))
        count = 0
        for idx, group in enumerate(groups):
            members = sorted({str(resource) for resource in group})
            if len(members) < 2:
                continue
            for resource in members:
                self._conn.execute(
                    "INSERT OR REPLACE INTO link_groups(client, group_idx, resource) VALUES(?, ?, ?)",
                    (client, idx, resource),
                )
            count += 1
        self._conn.commit()
        return count

    def link_groups_by_idx(self) -> dict[str, dict[int, list[str]]]:
        rows = self._conn.execute(
            "SELECT client, group_idx, resource FROM link_groups ORDER BY client, group_idx, resource"
        ).fetchall()
        grouped: dict[str, dict[int, list[str]]] = {}
        for client, group_idx, resource in rows:
            grouped.setdefault(str(client), {}).setdefault(int(group_idx), []).append(str(resource))
        return grouped

    def all_link_groups(self) -> dict[str, list[list[str]]]:
        return {
            client: [members for _idx, members in sorted(groups.items())]
            for client, groups in self.link_groups_by_idx().items()
        }

    def remove_link_group(self, client: str, group_idx: int) -> int:
        cur = self._conn.execute(
            "DELETE FROM link_groups WHERE client = ? AND group_idx = ?",
            (client, int(group_idx)),
        )
        self._conn.commit()
        return int(cur.rowcount)

    def close(self) -> None:
        self._conn.close()
