# -*- coding: utf-8 -*-
"""
    The following line helps to fix the Module Import issue when the Daemon tries
    to call the main module under this directory from outside.
"""
from pathlib import Path
import sys
#
# Insert the current directory into the PYTHONPATH
sys.path.insert(0, Path(__file__).parent.as_posix())