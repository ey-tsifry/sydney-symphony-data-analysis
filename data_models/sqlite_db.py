#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Note: DBRecord is currently a data class instead of a SQLAlchemy model because creating a full
SQLAlchemy model and its related scaffolding seemed like over-engineering for this project's
particular use case.

(Future migration to SQLAlchemy models is always possible if the use case becomes more complex).
"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class DBRecord:
    """
    Data class to represent a SSO HTML record in the SQLite DB.

    html_content is represented as a string so that it doesn't crash Pandas.
    """

    year: int
    key: str
    html_content: str

    @classmethod
    def fields(cls):
        """Return dictionary of data class fields."""
        field_dict: Dict = {field_name: field_name for field_name in cls.__dataclass_fields__}
        return field_dict
