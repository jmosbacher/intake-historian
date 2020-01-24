# -*- coding: utf-8 -*-

"""Top-level package for intake-xehistorian."""

__author__ = """Yossi Mosbacher"""
__email__ = 'joe.mosbacher@gmail.com'
__version__ = '0.1.0'

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .intake_xehistorian import XeHistorianSource
from .intake_xehistorian import XeHistorianDfSource
