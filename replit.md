# Automated Trading System

## Overview

This is an automated trading system that connects TradingView webhook signals to Tiger Securities for algorithmic trading execution. The system receives trade signals via HTTP webhooks, parses them, and automatically executes trades through the Tiger Securities API. It features a web-based dashboard for monitoring trades, configuring system settings, and tracking performance metrics.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture
- **Framework**: Flask web application with SQLAlchemy ORM for database operations
- **Database**: SQLite for development with configurable PostgreSQL support for production
- **Signal Processing**: Custom signal parser that handles TradingView webhook formats and normalizes trade data
- **Trading Integration**: Tiger Securities OpenAPI client for order execution and trade management
- **Configuration Management**: Hybrid configuration system using environment variables with database fallback

### Frontend Architecture
- **Template Engine**: Jinja2 templates with Bootstrap 5 dark theme for responsive UI
- **Dashboard**: Real-time trading statistics, recent trade history, and system status monitoring
- **Configuration Interface**: Web-based forms for API credential management and trading parameters
- **Trade Monitoring**: Comprehensive trade history with status tracking and order management

### Data Models
- **Trade Model**: Complete order lifecycle tracking including status, pricing, quantities, and Tiger API integration
- **TradingConfig Model**: Dynamic configuration storage for API credentials and system settings
- **SignalLog Model**: Webhook signal logging for debugging and audit trails

### Security and Reliability
- **Error Handling**: Comprehensive logging and error tracking throughout the signal-to-trade pipeline
- **Database Resilience**: Connection pooling with automatic reconnection and pre-ping validation
- **Production Ready**: ProxyFix middleware for proper deployment behind reverse proxies

## External Dependencies

### Trading Platform Integration
- **Tiger Securities OpenAPI**: Primary brokerage integration for order execution, account management, and market data
- **TradingView Webhooks**: Signal source for automated trading triggers and alert processing

### Development Framework
- **Flask**: Core web framework with SQLAlchemy for database operations
- **Bootstrap 5**: Frontend UI framework with dark theme optimization
- **Font Awesome**: Icon library for enhanced user interface elements

### Database Support
- **SQLite**: Default development database with seamless PostgreSQL migration path
- **PostgreSQL**: Production database option with advanced features and scalability

### JavaScript Libraries
- **Bootstrap JavaScript**: Interactive UI components and responsive behavior
- **Custom JavaScript**: Real-time status monitoring and dynamic content updates