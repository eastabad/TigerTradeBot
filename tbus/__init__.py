"""
TBUS Module - Independent protection order and market data system for Tiger US Standard accounts.

TBUS (US Standard) accounts have different capabilities from TBSG (HK Global):
- No OCA orders
- No attached orders  
- No STP_LMT order type
- GTC orders have outside_rth forced to False by server
- Need independent market data source (EODHD WebSocket)

This module provides:
- tbus_utils: TBUS account detection utilities
- tbus_client: Tiger API client for TBUS-specific order operations
- tbus_protection_service: Protection order lifecycle management
- tbus_quote_ws: EODHD WebSocket real-time market data client
"""
