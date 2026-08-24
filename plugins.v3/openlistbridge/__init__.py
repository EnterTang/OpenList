"""MoviePilot V3 host plugin for the OpenList Bridge protocol."""

from typing import Any, Optional

from apscheduler.triggers.interval import IntervalTrigger
from fastapi import Request
from starlette.responses import JSONResponse

from app.plugins import _PluginBase

from .openlistbridge.core import CONTROL_PATH, INTENT_PATH, SEARCH_PATH
from .openlistbridge.moviepilot import MoviePilotV3Gateway
from .openlistbridge.runtime import BridgeRuntime


class OpenListBridge(_PluginBase):
    """Expose HMAC-authenticated OpenList search and download-intent routes."""

    plugin_name = "OpenList Bridge"
    plugin_desc = "OpenList PT 资源搜索、qB 选择与状态中转（不订阅、不整理、不上传）"
    plugin_icon = "OpenList.png"
    plugin_version = "1.0.0"
    plugin_author = "OpenListTeam"
    author_url = "https://github.com/OpenListTeam/OpenList"
    plugin_config_prefix = "openlistbridge_"
    plugin_order = 40
    auth_level = 2
    max_body_bytes = 4 << 20

    _enabled = False
    _config: dict[str, Any] = {}
    runtime: Optional[BridgeRuntime] = None
    last_error = "插件未启用"

    def init_plugin(self, config: Optional[dict] = None) -> None:
        """Build runtime resources only after the plugin is explicitly enabled."""
        self.stop_service()
        self._config = dict(config or {})
        self._enabled = bool(self._config.get("enabled"))
        if not self._enabled:
            self.last_error = "插件未启用"
            return
        try:
            hmac_key = str(self._config.get("hmac_key", "")).encode()
            gateway = MoviePilotV3Gateway.from_moviepilot(
                hmac_key=hmac_key,
                save_path=str(self._config.get("save_path", "")).strip(),
            )
            self.runtime = BridgeRuntime.from_config(self._config, gateway=gateway)
            self.last_error = ""
        except Exception as exc:
            self.runtime = None
            self.last_error = str(exc)[:500]

    def get_state(self) -> bool:
        """Return true only when enabled configuration produced a runtime."""
        return self._enabled and self.runtime is not None

    @staticmethod
    def get_command() -> list[dict]:
        """The Bridge does not register MoviePilot chat commands."""
        return []

    def get_api(self) -> list[dict[str, Any]]:
        """Register routes under /api/v1/plugin/OpenListBridge.

        MoviePilot host authentication is intentionally omitted because these
        endpoints authenticate the exact raw request body with the independent
        per-instance HMAC protocol.
        """
        return [
            {
                "path": "/search",
                "endpoint": self.search,
                "methods": ["POST"],
                "summary": "OpenList 资源搜索",
                "response_model": None,
                "response_class": JSONResponse,
            },
            {
                "path": "/intent",
                "endpoint": self.submit_intent,
                "methods": ["POST"],
                "summary": "OpenList 下载意图",
                "response_model": None,
                "response_class": JSONResponse,
            },
            {
                "path": "/intent/{request_id}",
                "endpoint": self.intent_status,
                "methods": ["GET"],
                "summary": "OpenList 下载意图状态",
                "response_model": None,
                "response_class": JSONResponse,
            },
            {
                "path": "/intent/{request_id}/cancel",
                "endpoint": self.cancel_intent,
                "methods": ["POST"],
                "summary": "OpenList 取消未开始下载意图",
                "response_model": None,
                "response_class": JSONResponse,
            },
            {
                "path": "/control",
                "endpoint": self.control_torrent,
                "methods": ["POST"],
                "summary": "OpenList 精确种子控制",
                "response_model": None,
                "response_class": JSONResponse,
            },
        ]

    async def search(self, request: Request) -> JSONResponse:
        """Verify and execute one signed OpenList search request."""
        return await self._handle_request(request, SEARCH_PATH)

    async def submit_intent(self, request: Request) -> JSONResponse:
        """Verify and execute one signed, idempotent download intent."""
        return await self._handle_request(request, INTENT_PATH)

    async def intent_status(self, request_id: str, request: Request) -> JSONResponse:
        """Return one signed intent status without exposing torrent secrets."""
        return await self._handle_request(request, f"{INTENT_PATH}/{request_id}")

    async def cancel_intent(self, request_id: str, request: Request) -> JSONResponse:
        """Cancel an accepted intent before MoviePilot creates its download."""
        return await self._handle_request(request, f"{INTENT_PATH}/{request_id}/cancel")

    async def control_torrent(self, request: Request) -> JSONResponse:
        """Pause or resume one signed downloader/hash binding."""
        return await self._handle_request(request, CONTROL_PATH)

    async def _handle_request(self, request: Request, canonical_path: str) -> JSONResponse:
        if self.runtime is None:
            return JSONResponse({"error": self.last_error or "OpenList Bridge is unavailable"}, status_code=503)
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) < 0:
                    raise ValueError
            except ValueError:
                return JSONResponse({"error": "invalid content length"}, status_code=400)
            if int(content_length) > self.max_body_bytes:
                return JSONResponse({"error": "request body is too large"}, status_code=413)
        body = await request.body()
        if len(body) > self.max_body_bytes:
            return JSONResponse({"error": "request body is too large"}, status_code=413)
        response = self.runtime.core.handle(
            request.method,
            canonical_path,
            dict(request.headers.items()),
            body,
        )
        return JSONResponse(response.payload, status_code=response.status)

    def get_service(self) -> list[dict[str, Any]]:
        """Flush durable callbacks and poll bound torrents every minute."""
        if not self.get_state():
            return []
        return [{
            "id": "OpenListBridge.Tick",
            "name": "OpenList Bridge 状态与回调同步",
            "trigger": IntervalTrigger(minutes=1),
            "func": self.tick,
            "kwargs": {},
        }]

    def tick(self) -> int:
        """Run one idempotent state/outbox synchronization cycle."""
        return self.runtime.tick() if self.runtime is not None else 0

    def get_form(self) -> tuple[list[dict], dict[str, Any]]:
        """Return the MoviePilot-native configuration form."""
        return [{
            "component": "VForm",
            "content": [
                {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}},
                {"component": "VTextField", "props": {"model": "instance_id", "label": "MoviePilot 实例 ID"}},
                {"component": "VTextField", "props": {"model": "coordinator_url", "label": "OpenList Coordinator HTTPS 地址"}},
                {"component": "VTextField", "props": {"model": "hmac_key", "label": "实例独立 HMAC 密钥", "type": "password"}},
                {"component": "VTextField", "props": {"model": "state_directory", "label": "插件持久化目录"}},
                {"component": "VTextField", "props": {"model": "save_path", "label": "MoviePilot 下载目录（可选）"}},
                {"component": "VTextField", "props": {"model": "timeout_seconds", "label": "Coordinator 请求超时（秒）", "type": "number"}},
                {"component": "VTextField", "props": {"model": "retry_backoff_seconds", "label": "回调重试基础退避（秒）", "type": "number"}},
                {"component": "VTextField", "props": {"model": "creation_recovery_timeout_seconds", "label": "qB 绑定恢复超时（秒）", "type": "number"}},
            ],
        }], {
            "enabled": False,
            "instance_id": "",
            "coordinator_url": "",
            "hmac_key": "",
            "state_directory": "/config/plugins/openlistbridge",
            "save_path": "",
            "timeout_seconds": 15,
            "retry_backoff_seconds": 10,
            "creation_recovery_timeout_seconds": 600,
        }

    def get_page(self) -> list[dict]:
        """Expose a small health page without displaying secret values."""
        state = "运行中" if self.get_state() else "不可用"
        detail = state if not self.last_error else f"{state}：{self.last_error}"
        if self.runtime is not None:
            summary = self.runtime.core.status_summary()
            detail = f"{detail}；待回调 {summary['pending']}，已确认 {summary['acknowledged']}"
        return [{
            "component": "VAlert",
            "props": {
                "type": "success" if self.get_state() else "warning",
                "variant": "tonal",
                "text": detail,
            },
        }]

    def stop_service(self) -> None:
        """Release the in-process runtime; host-managed jobs stop separately."""
        self.runtime = None
        self._enabled = False


__all__ = ["OpenListBridge"]
