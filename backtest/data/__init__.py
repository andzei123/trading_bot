"""Data loading utilities.

This file fixes a packaging bug where the project shipped as
`backtest/data/_init_.py` instead of `__init__.py`, breaking imports like:

    from backtest.data.loader import ...
"""
