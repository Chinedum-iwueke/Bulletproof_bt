"""Centralized domain error taxonomy for Bulletproof_bt."""
from __future__ import annotations


class BtBaseError(Exception):
    """Base class for Bulletproof_bt domain errors."""


class ConfigError(BtBaseError, ValueError):
    pass


class DataError(BtBaseError, ValueError):
    pass


class StrategyContractError(BtBaseError, ValueError):
    pass


class RiskError(BtBaseError, ValueError):
    pass


class ExecutionError(BtBaseError, ValueError):
    pass


class PortfolioError(BtBaseError, ValueError):
    pass
