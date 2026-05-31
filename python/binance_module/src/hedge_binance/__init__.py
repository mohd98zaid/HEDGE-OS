"""PROJECT HEDGE — Binance Crypto Module.

A standalone async Python pipeline that connects to Binance WebSocket
streams and REST API.  Shares infrastructure (NATS, Redis, Postgres)
with the Indian-stock Hot_Path but uses dedicated NATS subjects
(``crypto.*``) so the two systems never interfere.
"""

__version__ = "0.1.0"
__all__: list[str] = []
