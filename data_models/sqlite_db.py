#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite DB record model
"""
from pydantic import BaseModel


class DBRecord(BaseModel):
    """
    Class to represent a SSO HTML record in the SQLite DB.

    html_content is represented as a string so that it doesn't crash Pandas.
    """

    year: int
    key: str
    html_content: str
