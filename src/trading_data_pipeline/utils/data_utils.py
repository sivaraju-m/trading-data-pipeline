"""
Data Utilities for Trading Data Pipeline
=======================================

Utility functions for data manipulation and validation.

Author: AI Trading Machine
Licensed by SJ Trading
"""

import logging
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np


def check_dataframe_validity(df: pd.DataFrame) -> bool:
    """
    Check if the dataframe is valid for processing.

    Args:
        df: DataFrame to check

    Returns:
        Boolean indicating if the dataframe is valid
    """
    if df is None:
        logging.warning("DataFrame is None")
        return False

    if len(df) == 0:
        logging.warning("DataFrame is empty")
        return False

    return True
