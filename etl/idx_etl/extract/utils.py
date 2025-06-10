"""
Utility functions for IDX Laporan Keuangan ETL process.
"""

from datetime import datetime


def get_current_quarter():
    """
    Determine the current quarter based on the current month
    
    Returns:
        tuple: (year, quarter) tuple where year is a string and quarter is an integer
    """
    CURRENT_YEAR_STR = str(datetime.now().year)
    CURRENT_MONTH = datetime.now().month
    QUARTER = 0

    if CURRENT_MONTH <= 3:
        QUARTER = 4
        CURRENT_YEAR_STR = str(int(CURRENT_YEAR_STR) - 1)
    elif CURRENT_MONTH <= 6:
        QUARTER = 1
    elif CURRENT_MONTH <= 9:
        QUARTER = 2
    else:
        QUARTER = 3
        
    return (CURRENT_YEAR_STR, QUARTER)
