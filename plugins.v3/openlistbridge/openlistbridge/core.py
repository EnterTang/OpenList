"""Framework-independent OpenList Bridge V1 implementation.

MoviePilot plugin SDK APIs are intentionally kept out of this module.  The
host adapter supplies a ``MoviePilotGateway`` that performs search and creates
the qB download, including the explicit request-to-hash binding required by
the OpenList protocol.
"""

import hashlib
import hmac
import json
import sqlite3
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Optional


SIGNATURE_VERSION = "v1"
MAX_CLOCK_SKEW_SECONDS = 300
NONCE_TTL_SECONDS = 600

PLUGIN_API_PREFIX = "/api/v1/plugin/OpenListBridge"
SEARCH_PATH = f"{PLUGIN_API_PREFIX}/search"
INTENT_PATH = f"{PLUGIN_API_PREFIX}/intent"
CONTROL_PATH = f"{PLUGIN_API_PREFIX}/control"

HEADER_VERSION = "X-OpenList-Bridge-Version"
HEADER_INSTANCE = "X-OpenList-Bridge-Instance"
HEADER_TIMESTAMP = "X-OpenList-Bridge-Timestamp"
HEADER_NONCE = "X-OpenList-Bridge-Nonce"
HEADER_SIGNATURE = "X-OpenList-Bridge-Signature"

FORBIDDEN_BRIDGE_FIELDS = frozenset({"site_cookie", "qb_password", "qb_url", "local_path", "enclosure"})


@dataclass(frozen=True)
class CreatedTorrent:
    downloader: str
    torrent_hash: str
    content_path: str
    size: int = 0


@dataclass(frozen=True)
class TorrentState:
    state: str
    progress: float
    left_time: int = 0
    ratio: float = 0.0
    seeding_seconds: int = 0
    hnr_passed: Optional[bool] = None


@dataclass(frozen=True)
class BridgeResponse:
    status: int
    payload: dict


class MoviePilotGateway(ABC):
    """The narrow MoviePilot-specific surface required by the Bridge."""

    @abstractmethod
    def search(self, request: dict) -> list[dict]:
        """Return sanitized, opaque resource search results."""

    @abstractmethod
    def create_download(self, intent: dict) -> CreatedTorrent:
        """Create a download and return its resolved qB binding.

        Returning a qB hash from an add-download response is not enough unless
        the adapter obtained it from an explicit MoviePilot/qB association.
        Snapshot matching against a download list is intentionally forbidden.
        """

    def get_torrent_state(self, torrent: CreatedTorrent) -> Optional[TorrentState]:
        """Return the current state for an exact downloader/hash binding."""
        return None

    def recover_download(self, intent: dict) -> Optional[CreatedTorrent]:
        """Recover a request-specific qB binding after a plugin restart."""
        return None

    def control_torrent(self, torrent: CreatedTorrent, action: str) -> None:
        """Pause or resume one exact downloader/hash binding."""
        raise NotImplementedError("MoviePilot gateway does not support torrent control")


class BridgeCore:
    def __init__(
        self,
        *,
        instance_id: str,
        hmac_key: bytes,
        database_path: Path,
        gateway: MoviePilotGateway,
        coordinator_event_sender: Callable[[dict, str], None],
        now: Optional[Callable[[], int]] = None,
        retry_backoff_seconds: int = 10,
        creation_recovery_timeout_seconds: int = 600,
    ) -> None:
        if not instance_id.strip() or len(hmac_key) < 16:
            raise ValueError("instance_id and a 16-byte hmac_key are required")
        self.instance_id = instance_id.strip()
        self.hmac_key = hmac_key
        self.gateway = gateway
        self.coordinator_event_sender = coordinator_event_sender
        self.now = now or (lambda: int(time.time()))
        self.retry_backoff_seconds = max(1, min(int(retry_backoff_seconds), 3600))
        self.creation_recovery_timeout_seconds = max(
            60, min(int(creation_recovery_timeout_seconds), 86400)
        )
        self.database_path = str(database_path)
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize_database(self) -> None:
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS nonces (
                    instance_id TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    PRIMARY KEY (instance_id, nonce)
                );
                CREATE TABLE IF NOT EXISTS intents (
                    request_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    available_at INTEGER NOT NULL DEFAULT 0,
                    acknowledged_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bindings (
                    request_id TEXT PRIMARY KEY,
                    downloader TEXT NOT NULL,
                    torrent_hash TEXT NOT NULL,
                    content_path TEXT NOT NULL,
                    size INTEGER NOT NULL DEFAULT 0,
                    last_state_json TEXT NOT NULL DEFAULT '',
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY (request_id) REFERENCES intents(request_id)
                );
                """
            )
            self._ensure_column(conn, "outbox", "available_at", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "outbox", "acknowledged_at", "INTEGER")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def handle(self, method: str, path: str, headers: Mapping[str, str], body: bytes) -> BridgeResponse:
        method = method.upper().strip()
        try:
            self._verify_and_record_nonce(method, path, headers, body)
        except ValueError as exc:
            return BridgeResponse(401, {"error": str(exc)})
        if method == "POST" and path == SEARCH_PATH:
            return self._search(body)
        if method == "POST" and path == INTENT_PATH:
            return self._submit_intent(body)
        if method == "POST" and path == CONTROL_PATH:
            return self._control_torrent(body)
        if method == "GET" and path.startswith(f"{INTENT_PATH}/"):
            return self._get_intent(path.rsplit("/", 1)[-1])
        if method == "POST" and path.startswith(f"{INTENT_PATH}/") and path.endswith("/cancel"):
            return self._cancel_intent(path.split("/")[-2])
        return BridgeResponse(404, {"error": "OpenList Bridge endpoint was not found"})

    def _control_torrent(self, body: bytes) -> BridgeResponse:
        try:
            request = self._decode_json(body)
            request_id = str(request.get("request_id", "")).strip()
            downloader = str(request.get("downloader", "")).strip()
            torrent_hash = str(request.get("torrent_hash", "")).strip().lower()
            action = str(request.get("action", "")).strip().lower()
            if not request_id or not downloader or not torrent_hash or action not in {"pause", "resume"}:
                raise ValueError("request_id, downloader, torrent_hash and a pause/resume action are required")
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT downloader, torrent_hash, content_path, size FROM bindings WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
            if row is None:
                return BridgeResponse(404, {"error": "torrent binding was not found"})
            if row["downloader"].strip().lower() != downloader.lower() or row["torrent_hash"].strip().lower() != torrent_hash:
                return BridgeResponse(409, {"error": "torrent control does not match the persisted binding"})
            binding = CreatedTorrent(
                downloader=row["downloader"], torrent_hash=row["torrent_hash"],
                content_path=row["content_path"], size=row["size"],
            )
            self.gateway.control_torrent(binding, action)
            return BridgeResponse(200, {"request_id": request_id, "action": action, "status": "accepted"})
        except ValueError as exc:
            return BridgeResponse(400, {"error": str(exc)})
        except Exception:
            return BridgeResponse(502, {"error": "MoviePilot torrent control failed"})

    def _verify_and_record_nonce(self, method: str, path: str, headers: Mapping[str, str], body: bytes) -> None:
        def header(name: str) -> str:
            for key, value in headers.items():
                if key.lower() == name.lower():
                    return str(value).strip()
            return ""

        version = header(HEADER_VERSION)
        instance_id = header(HEADER_INSTANCE)
        timestamp_raw = header(HEADER_TIMESTAMP)
        nonce = header(HEADER_NONCE)
        signature = header(HEADER_SIGNATURE)
        if version != SIGNATURE_VERSION or instance_id != self.instance_id or not nonce or not signature:
            raise ValueError("bridge signature headers are invalid")
        try:
            timestamp = int(timestamp_raw)
        except ValueError as exc:
            raise ValueError("bridge timestamp is invalid") from exc
        now = self.now()
        if abs(now - timestamp) > MAX_CLOCK_SKEW_SECONDS:
            raise ValueError("bridge timestamp is outside the allowed clock skew")
        body_hash = hashlib.sha256(body).hexdigest()
        canonical = "\n".join((version, instance_id, method, path, str(timestamp), nonce, body_hash)).encode()
        expected = hmac.new(self.hmac_key, canonical, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("invalid bridge signature")
        with self._connect() as conn:
            conn.execute("DELETE FROM nonces WHERE expires_at < ?", (now,))
            try:
                conn.execute(
                    "INSERT INTO nonces(instance_id, nonce, expires_at) VALUES (?, ?, ?)",
                    (self.instance_id, nonce, now + NONCE_TTL_SECONDS),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("bridge nonce has already been used") from exc

    def _decode_json(self, body: bytes) -> dict:
        try:
            value = json.loads(body.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("request body must be a JSON object") from exc
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        self._reject_forbidden_fields(value)
        return value

    @staticmethod
    def _reject_forbidden_fields(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).strip().lower() in FORBIDDEN_BRIDGE_FIELDS:
                    raise ValueError(f"bridge payload contains forbidden field {key!r}")
                BridgeCore._reject_forbidden_fields(child)
        elif isinstance(value, list):
            for child in value:
                BridgeCore._reject_forbidden_fields(child)

    def _search(self, body: bytes) -> BridgeResponse:
        try:
            request = self._decode_json(body)
            if not all(str(request.get(key, "")).strip() for key in ("request_id", "query", "media_source", "media_id")):
                raise ValueError("search request_id, query, media_source and media_id are required")
            results = self.gateway.search(request)
            return BridgeResponse(200, {"results": [self._sanitize_result(item) for item in results]})
        except ValueError as exc:
            return BridgeResponse(400, {"error": str(exc)})
        except Exception:
            return BridgeResponse(502, {"error": "MoviePilot search failed"})

    def _submit_intent(self, body: bytes) -> BridgeResponse:
        try:
            intent = self._decode_json(body)
            self._validate_intent(intent)
        except ValueError as exc:
            return BridgeResponse(400, {"error": str(exc)})
        request_id = intent["request_id"].strip()
        now = self.now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT status, payload_json FROM intents WHERE request_id = ?", (request_id,)
            ).fetchone()
            if existing is not None:
                persisted_intent = json.loads(existing["payload_json"])
                if persisted_intent != intent:
                    return BridgeResponse(
                        409, {"error": "request_id already belongs to a different intent"}
                    )
                status = existing["status"]
                if status in {"accepted", "creating"}:
                    try:
                        recovered = self.gateway.recover_download(persisted_intent)
                        if recovered is not None:
                            self._validate_created_torrent(recovered)
                            self._persist_created_torrent(persisted_intent, recovered)
                            self.poll_torrent_states(request_id=request_id)
                            self.flush_outbox()
                            status = "bound"
                    except Exception:
                        pass
                return BridgeResponse(202, {"request_id": request_id, "status": status, "duplicate": True})
            conn.execute(
                "INSERT INTO intents(request_id, payload_json, status, created_at, updated_at) VALUES (?, ?, 'accepted', ?, ?)",
                (request_id, json.dumps(intent, sort_keys=True, separators=(",", ":")), now, now),
            )
            self._enqueue_event_tx(conn, request_id, "intent.accepted")
            conn.execute("UPDATE intents SET status = 'creating', updated_at = ? WHERE request_id = ?", (now, request_id))
        try:
            created = self.gateway.create_download(intent)
            self._validate_created_torrent(created)
            self._persist_created_torrent(intent, created)
            self.poll_torrent_states(request_id=request_id)
        except Exception:
            recovered = None
            try:
                recovered = self.gateway.recover_download(intent)
                if recovered is not None:
                    self._validate_created_torrent(recovered)
            except Exception:
                recovered = None
            if recovered is not None:
                self._persist_created_torrent(intent, recovered)
                self.poll_torrent_states(request_id=request_id)
        self.flush_outbox()
        return BridgeResponse(202, {"request_id": request_id, "status": self._intent_status(request_id)})

    def _persist_created_torrent(self, intent: dict, created: CreatedTorrent) -> None:
        request_id = intent["request_id"].strip()
        now = self.now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT downloader, torrent_hash, content_path, size FROM bindings WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                persisted = (
                    str(existing["downloader"]), str(existing["torrent_hash"]),
                    str(existing["content_path"]), int(existing["size"]),
                )
                candidate = (
                    created.downloader, created.torrent_hash, created.content_path, int(created.size),
                )
                if persisted != candidate:
                    raise ValueError("request_id already has a different immutable torrent binding")
                conn.execute(
                    "UPDATE intents SET status = 'bound', updated_at = ? WHERE request_id = ?",
                    (now, request_id),
                )
                return
            conn.execute(
                """
                INSERT INTO bindings(request_id, downloader, torrent_hash, content_path, size, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (request_id, created.downloader, created.torrent_hash, created.content_path, created.size, now),
            )
            self._enqueue_event_tx(conn, request_id, "torrent.bound", torrent={
                "downloader": created.downloader,
                "torrent_hash": created.torrent_hash,
                "content_path": created.content_path,
                "size": created.size,
                "media": intent.get("media", {}),
            })
            conn.execute("UPDATE intents SET status = 'bound', updated_at = ? WHERE request_id = ?", (now, request_id))

    def recover_pending_intents(self, limit: int = 100) -> int:
        """Recover uncertain qB creates before declaring them failed.

        A qB add can succeed even when MoviePilot's immediate list lookup times
        out. The request-specific label makes a later lookup deterministic.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT request_id, payload_json, created_at FROM intents WHERE status = 'creating' ORDER BY created_at, request_id LIMIT ?",
                (max(1, limit),),
            ).fetchall()
        recovered_count = 0
        for row in rows:
            intent = json.loads(row["payload_json"])
            recovered = None
            try:
                recovered = self.gateway.recover_download(intent)
                if recovered is not None:
                    self._validate_created_torrent(recovered)
            except Exception:
                recovered = None
            if recovered is not None:
                self._persist_created_torrent(intent, recovered)
                self.poll_torrent_states(request_id=row["request_id"])
                recovered_count += 1
                continue
            if self.now() - int(row["created_at"]) < self.creation_recovery_timeout_seconds:
                continue
            with self._connect() as conn:
                current = conn.execute(
                    "SELECT status FROM intents WHERE request_id = ?", (row["request_id"],)
                ).fetchone()
                if current is None or current["status"] != "creating":
                    continue
                self._enqueue_event_tx(conn, row["request_id"], "torrent.failed", failure={
                    "code": "download_binding_timeout",
                    "message": "MoviePilot could not recover the qB binding before the recovery timeout",
                })
                conn.execute(
                    "UPDATE intents SET status = 'failed', updated_at = ? WHERE request_id = ? AND status = 'creating'",
                    (self.now(), row["request_id"]),
                )
        return recovered_count

    def _get_intent(self, request_id: str) -> BridgeResponse:
        with self._connect() as conn:
            row = conn.execute("SELECT status FROM intents WHERE request_id = ?", (request_id,)).fetchone()
        if row is None:
            return BridgeResponse(404, {"error": "intent was not found"})
        return BridgeResponse(200, {"request_id": request_id, "status": row["status"]})

    def _cancel_intent(self, request_id: str) -> BridgeResponse:
        with self._connect() as conn:
            result = conn.execute("UPDATE intents SET status = 'cancelled', updated_at = ? WHERE request_id = ? AND status = 'accepted'", (self.now(), request_id))
        if result.rowcount == 0:
            return BridgeResponse(409, {"error": "only an accepted intent can be cancelled"})
        return BridgeResponse(200, {"request_id": request_id, "status": "cancelled"})

    def _enqueue_event(
        self,
        request_id: str,
        event_type: str,
        *,
        torrent: Optional[dict] = None,
        state: Optional[dict] = None,
        failure: Optional[dict] = None,
    ) -> None:
        with self._connect() as conn:
            self._enqueue_event_tx(conn, request_id, event_type, torrent=torrent, state=state, failure=failure)

    def _enqueue_event_tx(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        event_type: str,
        *,
        torrent: Optional[dict] = None,
        state: Optional[dict] = None,
        failure: Optional[dict] = None,
    ) -> None:
        now = self.now()
        event_id = str(uuid.uuid4())
        event = {"event_id": event_id, "request_id": request_id, "type": event_type, "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))}
        if torrent is not None:
            event["torrent"] = torrent
        if state is not None:
            event["state"] = state
        if failure is not None:
            event["failure"] = failure
        conn.execute(
            "INSERT INTO outbox(event_id, request_id, payload_json, status, available_at, created_at, updated_at) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
            (event_id, request_id, json.dumps(event, separators=(",", ":")), now, now, now),
        )

    def poll_torrent_states(self, request_id: Optional[str] = None, limit: int = 100) -> int:
        """Enqueue deduplicated state events for exact downloader/hash bindings."""
        query = """
            SELECT b.request_id, b.downloader, b.torrent_hash, b.content_path,
                   b.size, b.last_state_json
            FROM bindings b
            JOIN intents i ON i.request_id = b.request_id
            WHERE i.status NOT IN ('failed', 'cancelled')
        """
        parameters: list[object] = []
        if request_id:
            query += " AND b.request_id = ?"
            parameters.append(request_id)
        query += " ORDER BY b.updated_at, b.request_id LIMIT ?"
        parameters.append(max(1, limit))
        with self._connect() as conn:
            rows = conn.execute(query, parameters).fetchall()
        changed = 0
        for row in rows:
            binding = CreatedTorrent(
                downloader=row["downloader"],
                torrent_hash=row["torrent_hash"],
                content_path=row["content_path"],
                size=row["size"],
            )
            try:
                snapshot = self.gateway.get_torrent_state(binding)
            except Exception:
                continue
            if snapshot is None:
                continue
            state = {
                "state": str(snapshot.state or "unknown"),
                "progress": max(0.0, min(float(snapshot.progress), 1.0)),
                "left_time": max(0, int(snapshot.left_time)),
                "ratio": max(0.0, float(snapshot.ratio)),
                "seeding_seconds": max(0, int(snapshot.seeding_seconds)),
            }
            if snapshot.hnr_passed is not None:
                state["hnr_passed"] = bool(snapshot.hnr_passed)
            state_json = json.dumps(state, sort_keys=True, separators=(",", ":"))
            if hmac.compare_digest(state_json, row["last_state_json"] or ""):
                continue
            self._enqueue_event(row["request_id"], "torrent.state_changed", state=state)
            normalized = state["state"].strip().lower()
            status = "completed" if normalized in {
                "completed", "complete", "seeding", "uploading", "stalledup", "queuedup",
                "forcedup", "checkingup", "pausedup", "stoppedup", "stalled_seeding",
            } or state["progress"] >= 1 else "downloading"
            with self._connect() as conn:
                conn.execute(
                    "UPDATE bindings SET last_state_json = ?, updated_at = ? WHERE request_id = ?",
                    (state_json, self.now(), row["request_id"]),
                )
                conn.execute(
                    "UPDATE intents SET status = ?, updated_at = ? WHERE request_id = ?",
                    (status, self.now(), row["request_id"]),
                )
            changed += 1
        return changed

    def flush_outbox(self, limit: int = 20) -> int:
        sent = 0
        with self._connect() as conn:
            # SQLite rowid retains the enqueue sequence even when multiple
            # events share the same second. intent.accepted must reach the
            # Coordinator before its subsequent torrent.bound event.
            rows = conn.execute(
                "SELECT event_id, payload_json, attempt_count, available_at FROM outbox WHERE status = 'pending' ORDER BY rowid LIMIT ?",
                (limit,),
            ).fetchall()
        for row in rows:
            if row["available_at"] > self.now():
                break
            event = json.loads(row["payload_json"])
            try:
                self.coordinator_event_sender(event, row["event_id"])
            except Exception as exc:
                delay = min(900, self.retry_backoff_seconds * (2 ** min(int(row["attempt_count"]), 7)))
                with self._connect() as conn:
                    conn.execute(
                        "UPDATE outbox SET attempt_count = attempt_count + 1, available_at = ?, last_error = ?, updated_at = ? WHERE event_id = ?",
                        (self.now() + delay, str(exc)[:500], self.now(), row["event_id"]),
                    )
                break
            with self._connect() as conn:
                conn.execute(
                    "UPDATE outbox SET status = 'acknowledged', acknowledged_at = ?, attempt_count = attempt_count + 1, last_error = '', updated_at = ? WHERE event_id = ?",
                    (self.now(), self.now(), row["event_id"]),
                )
            sent += 1
        return sent

    def status_summary(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM outbox GROUP BY status").fetchall()
        counts = {row["status"]: int(row["count"]) for row in rows}
        return {
            "pending": counts.get("pending", 0),
            "acknowledged": counts.get("acknowledged", 0) + counts.get("sent", 0),
        }

    def _set_intent_status(self, request_id: str, status: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE intents SET status = ?, updated_at = ? WHERE request_id = ?", (status, self.now(), request_id))

    def _intent_status(self, request_id: str) -> str:
        with self._connect() as conn:
            row = conn.execute("SELECT status FROM intents WHERE request_id = ?", (request_id,)).fetchone()
        return row["status"] if row else "unknown"

    @staticmethod
    def _sanitize_result(value: dict) -> dict:
        allowed = ("resource_ref", "title", "site", "size", "seeders", "leechers", "selected_fingerprint")
        result = {key: value[key] for key in allowed if key in value}
        BridgeCore._validate_opaque_resource_ref(result.get("resource_ref"))
        return result

    @staticmethod
    def _validate_opaque_resource_ref(value: object) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("torrent resource_ref is required")
        normalized = value.strip()
        lowered = normalized.lower()
        if (
            len(normalized) > 2048
            or "://" in normalized
            or lowered.startswith("magnet:")
            or any(character in normalized for character in ("\r", "\n", "\x00"))
        ):
            raise ValueError(
                "torrent resource_ref must be an opaque Bridge reference, not a download URL"
            )

    @staticmethod
    def _validate_intent(intent: dict) -> None:
        request_id = intent.get("request_id")
        media = intent.get("media")
        torrent = intent.get("torrent")
        policy = intent.get("downloader_policy")
        if not isinstance(request_id, str) or not request_id.strip() or not isinstance(media, dict) or not isinstance(torrent, dict) or not isinstance(policy, dict):
            raise ValueError("request_id, media, torrent and downloader_policy are required")
        if not all(isinstance(media.get(key), str) and media[key].strip() for key in ("media_source", "media_id")):
            raise ValueError("media_source and media_id are required")
        if policy.get("mode") != "moviepilot_select":
            raise ValueError("downloader_policy.mode must be moviepilot_select")
        BridgeCore._validate_opaque_resource_ref(torrent.get("resource_ref"))

    @staticmethod
    def _validate_created_torrent(created: CreatedTorrent) -> None:
        if not created.downloader.strip() or not created.content_path.startswith("/") or created.content_path == "/":
            raise ValueError("MoviePilot did not resolve downloader and content_path")
        torrent_hash = created.torrent_hash.strip()
        if len(torrent_hash) not in (40, 64) or torrent_hash.lower() != torrent_hash or any(char not in "0123456789abcdef" for char in torrent_hash):
            raise ValueError("MoviePilot did not resolve a valid lowercase qB torrent hash")
