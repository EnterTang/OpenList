"""MoviePilot-facing gateways with exact qB hash/downloader binding."""

import hashlib
import hmac
import json
import time
from typing import Callable, Optional

from .core import CreatedTorrent, MoviePilotGateway, TorrentState


class MoviePilotGatewayAdapter(MoviePilotGateway):
    def __init__(self, *, search_resources: Callable[[dict], list[dict]], create_download_and_bind: Callable[[dict], dict]) -> None:
        self._search_resources = search_resources
        self._create_download_and_bind = create_download_and_bind

    def search(self, request: dict) -> list[dict]:
        return self._search_resources(request)

    def create_download(self, intent: dict) -> CreatedTorrent:
        binding = self._create_download_and_bind(intent)
        if not isinstance(binding, dict):
            raise ValueError("MoviePilot binding provider returned an invalid result")
        return CreatedTorrent(
            downloader=str(binding.get("downloader", "")),
            torrent_hash=str(binding.get("torrent_hash", "")),
            content_path=str(binding.get("content_path", "")),
            size=int(binding.get("size", 0)),
        )


class MoviePilotV3Gateway(MoviePilotGateway):
    """Use MoviePilot V3 search/download chains without list-diff guessing.

    ``download_single`` returns the qB hash created by the same synchronous
    request. ``list_torrents`` is then filtered by that exact hash to recover
    the downloader alias and content path selected by MoviePilot.
    """

    def __init__(
        self,
        *,
        search_chain,
        download_chain,
        hmac_key: bytes,
        save_path: str = "",
        media_type_resolver: Optional[Callable[[str], object]] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if len(hmac_key) < 16:
            raise ValueError("a 16-byte hmac_key is required")
        self._search_chain = search_chain
        self._download_chain = download_chain
        self._hmac_key = hmac_key
        self._save_path = str(save_path or "").strip()
        self._media_type_resolver = media_type_resolver or self._default_media_type
        self._sleep = sleep
        self._contexts: dict[str, object] = {}
        self._fingerprints: dict[str, str] = {}

    @classmethod
    def from_moviepilot(cls, *, hmac_key: bytes, save_path: str = "") -> "MoviePilotV3Gateway":
        """Construct the adapter lazily inside an initialized V3 host."""
        from app.chain.download import DownloadChain
        from app.chain.search import SearchChain

        return cls(
            search_chain=SearchChain(),
            download_chain=DownloadChain(),
            hmac_key=hmac_key,
            save_path=save_path,
        )

    @staticmethod
    def _default_media_type(value: str):
        from app.schemas.types import MediaType

        normalized = str(value or "").strip().lower()
        if not normalized:
            return None
        aliases = {
            "movie": getattr(MediaType, "MOVIE", None),
            "tv": getattr(MediaType, "TV", None),
        }
        if normalized in aliases and aliases[normalized] is not None:
            return aliases[normalized]
        try:
            return MediaType(normalized)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _value(obj, name: str, default=None):
        if isinstance(obj, dict):
            return obj.get(name, default)
        return getattr(obj, name, default)

    def _resource_identity(self, context) -> tuple[str, str]:
        torrent = self._value(context, "torrent_info")
        descriptor = {
            "title": str(self._value(torrent, "title", "") or "").strip(),
            "site": str(self._value(torrent, "site_name", "") or "").strip(),
            "size": self._int(self._value(torrent, "size", 0)),
            "page": str(self._value(torrent, "page_url", "") or "").strip(),
        }
        canonical = json.dumps(descriptor, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        fingerprint = hashlib.sha256(canonical).hexdigest()
        opaque = hmac.new(self._hmac_key, b"resource-ref-v1\n" + canonical, hashlib.sha256).hexdigest()
        return f"olb:v1:{opaque}", fingerprint

    def search(self, request: dict) -> list[dict]:
        source = str(request.get("media_source", "")).strip()
        media_id = str(request.get("media_id", "")).strip()
        if not source or not media_id:
            raise ValueError("media_source and media_id are required")
        media_type = self._media_type_resolver(str(request.get("media_type", "")))
        season = self._int(request.get("season")) or None
        contexts = self._search_chain.search_by_id(
            source=source,
            mediaid=media_id,
            mtype=media_type,
            season=season,
            sites=None,
            cache_local=False,
        ) or []
        results = []
        for context in contexts:
            torrent = self._value(context, "torrent_info")
            if torrent is None:
                continue
            resource_ref, fingerprint = self._resource_identity(context)
            self._contexts[resource_ref] = context
            self._fingerprints[resource_ref] = fingerprint
            results.append({
                "resource_ref": resource_ref,
                "title": str(self._value(torrent, "title", "") or "").strip(),
                "site": str(self._value(torrent, "site_name", "") or "").strip(),
                "size": self._int(self._value(torrent, "size", 0)),
                "seeders": self._int(self._value(torrent, "seeders", 0)),
                "leechers": self._int(self._value(torrent, "leechers", 0)),
                "selected_fingerprint": fingerprint,
            })
        return results

    def create_download(self, intent: dict) -> CreatedTorrent:
        torrent_request = intent.get("torrent") or {}
        resource_ref = str(torrent_request.get("resource_ref", "")).strip()
        if not resource_ref:
            raise ValueError("MoviePilot V3 downloads require an opaque resource_ref")
        context = self._contexts.get(resource_ref)
        if context is None:
            media = intent.get("media") or {}
            replay = {
                "request_id": intent.get("request_id"),
                "query": torrent_request.get("title") or media.get("title") or resource_ref,
                "media_source": media.get("media_source"),
                "media_id": media.get("media_id"),
                "media_type": media.get("media_type"),
                "season": media.get("season"),
            }
            self.search(replay)
            context = self._contexts.get(resource_ref)
        if context is None:
            raise ValueError("selected MoviePilot resource is no longer available")
        selected_fingerprint = str(torrent_request.get("selected_fingerprint", "")).strip()
        if selected_fingerprint and not hmac.compare_digest(
            selected_fingerprint, self._fingerprints.get(resource_ref, "")
        ):
            raise ValueError("selected MoviePilot resource fingerprint changed")
        result = self._download_chain.download_single(
            context,
            episodes=None,
            source=f"OpenList:{str(intent.get('request_id', '')).strip()}",
            username="OpenListBridge",
            label=self._request_label(str(intent.get("request_id", ""))),
            save_path=self._save_path or None,
            return_detail=True,
        )
        if isinstance(result, tuple):
            torrent_hash, error_message = result
        else:
            torrent_hash, error_message = result, ""
        torrent_hash = str(torrent_hash or "").strip().lower()
        if not torrent_hash:
            raise ValueError(str(error_message or "MoviePilot did not create a download"))
        bound = self._find_exact_torrent(torrent_hash, downloader=None)
        downloader = str(self._value(bound, "downloader", "") or "").strip()
        content_path = str(
            self._value(bound, "content_path", "")
            or self._value(bound, "path", "")
            or ""
        ).strip()
        if not downloader or not content_path:
            raise ValueError("MoviePilot did not expose the exact qB downloader/content path binding")
        return CreatedTorrent(
            downloader=downloader,
            torrent_hash=torrent_hash,
            content_path=content_path,
            size=self._int(self._value(bound, "size", 0)),
        )

    def recover_download(self, intent: dict) -> Optional[CreatedTorrent]:
        """Recover an interrupted create call by its unique qB label.

        MoviePilot forwards ``label`` to the selected downloader. Scanning for
        this request-specific label is deterministic and avoids the unsafe
        before/after torrent-list difference heuristic.
        """
        request_id = str(intent.get("request_id", "")).strip()
        if not request_id:
            return None
        label = self._request_label(request_id)
        matches = []
        for item in self._download_chain.list_torrents(include_all_tags=True) or []:
            if label in self._torrent_tags(item):
                matches.append(item)
        if not matches:
            return None
        if len(matches) != 1:
            raise ValueError("MoviePilot returned multiple qB torrents for one OpenList request label")
        bound = matches[0]
        downloader = str(self._value(bound, "downloader", "") or "").strip()
        torrent_hash = str(self._value(bound, "hash", "") or "").strip().lower()
        content_path = str(
            self._value(bound, "content_path", "")
            or self._value(bound, "path", "")
            or ""
        ).strip()
        if not downloader or not torrent_hash or not content_path:
            raise ValueError("MoviePilot recovery result has no exact downloader/hash/content path binding")
        return CreatedTorrent(
            downloader=downloader,
            torrent_hash=torrent_hash,
            content_path=content_path,
            size=self._int(self._value(bound, "size", 0)),
        )

    @staticmethod
    def _request_label(request_id: str) -> str:
        digest = hashlib.sha256(str(request_id or "").strip().encode()).hexdigest()[:24]
        return f"OPENLIST_{digest}"

    @classmethod
    def _torrent_tags(cls, torrent) -> set[str]:
        raw = cls._value(torrent, "tags", cls._value(torrent, "labels", cls._value(torrent, "tag", "")))
        if isinstance(raw, str):
            return {value.strip() for value in raw.split(",") if value.strip()}
        if isinstance(raw, (list, tuple, set)):
            return {str(value).strip() for value in raw if str(value).strip()}
        return set()

    def get_torrent_state(self, torrent: CreatedTorrent) -> Optional[TorrentState]:
        try:
            bound = self._find_exact_torrent(torrent.torrent_hash, downloader=torrent.downloader)
        except ValueError:
            return None
        progress = self._float(self._value(bound, "progress", 0))
        if progress > 1:
            progress /= 100
        progress = max(0.0, min(progress, 1.0))
        return TorrentState(
            state=str(self._value(bound, "state", "unknown") or "unknown"),
            progress=progress,
            left_time=self._int(self._value(bound, "left_time", 0)),
            ratio=self._float(self._value(bound, "ratio", 0)),
            seeding_seconds=self._int(
                self._value(bound, "seeding_seconds", self._value(bound, "seeding_time", 0))
            ),
            hnr_passed=self._optional_bool(self._value(bound, "hnr_passed", None)),
        )

    def control_torrent(self, torrent: CreatedTorrent, action: str) -> None:
        action = str(action or "").strip().lower()
        arguments = {"hashs": [torrent.torrent_hash], "downloader": torrent.downloader}
        if action == "pause":
            result = self._download_chain.stop_torrents(**arguments)
        elif action == "resume":
            result = self._download_chain.start_torrents(**arguments)
        else:
            raise ValueError("unsupported torrent control action")
        if result is False:
            raise ValueError(f"MoviePilot rejected torrent {action}")

    def _find_exact_torrent(self, torrent_hash: str, downloader: Optional[str]):
        arguments = {"hashs": [torrent_hash], "include_all_tags": True}
        if downloader:
            arguments["downloader"] = downloader
        for attempt in range(3):
            matches = self._download_chain.list_torrents(**arguments) or []
            exact = [item for item in matches if str(self._value(item, "hash", "")).strip().lower() == torrent_hash]
            if len(exact) == 1:
                return exact[0]
            if len(exact) > 1:
                raise ValueError("MoviePilot returned multiple qB bindings for one torrent hash")
            if attempt < 2:
                self._sleep(0.2)
        raise ValueError("MoviePilot could not resolve the created qB torrent by exact hash")

    @staticmethod
    def _int(value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _float(value) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _optional_bool(value) -> Optional[bool]:
        return value if isinstance(value, bool) else None
