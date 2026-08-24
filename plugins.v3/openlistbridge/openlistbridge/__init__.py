"""OpenList control-plane bridge for MoviePilot plugins."""

from .core import BridgeCore, BridgeResponse, CreatedTorrent, MoviePilotGateway
from .moviepilot import MoviePilotGatewayAdapter
from .plugin import OpenListBridgePlugin
from .runtime import BridgeRuntime

__all__ = (
    "BridgeCore",
    "BridgeResponse",
    "BridgeRuntime",
    "CreatedTorrent",
    "MoviePilotGateway",
    "MoviePilotGatewayAdapter",
    "OpenListBridgePlugin",
)
