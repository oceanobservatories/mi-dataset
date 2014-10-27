"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/utilities.py
@author Joe Padula
@brief Utilities that can be used by any parser
Release notes:

initial release
"""

__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

from datetime import datetime, timedelta
import ntplib
import calendar
import time
from dateutil import parser

from mi.core.log import get_logger
log = get_logger()

# Format of DCL Controller Timestamp in records
# Example: 2014/08/17 00:57:10.648
ZULU_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# Format of DCL Controller Timestamp in records
# Example: 2014/08/17 00:57:10.648
DCL_CONTROLLER_TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S.%f"

def formatted_timestamp_utc_time(timestamp_str, format_str):

    dt = datetime.strptime(timestamp_str, format_str)

    return calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.0)

def zulu_timestamp_to_utc_time(zulu_timestamp_str):
    """
    Converts a zulu formatted timestamp timestamp string to UTC time.
    :param zulu_timestamp_str: a zulu formatted timestamp string
    :return: UTC time in seconds and microseconds precision
    """

    return formatted_timestamp_utc_time(zulu_timestamp_str,
                                        ZULU_TIMESTAMP_FORMAT)


def zulu_timestamp_to_ntp_time(zulu_timestamp_str):
    """
    Converts a zulu formatted timestamp timestamp string to NTP time.
    :param zulu_timestamp_str: a zulu formatted timestamp string
    :return: NTP time in seconds and microseconds precision
    """

    utc_time = zulu_timestamp_to_utc_time(zulu_timestamp_str)

    return float(ntplib.system_to_ntp_time(utc_time))


def dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to UTC time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: UTC time in seconds and microseconds precision
    """


    return formatted_timestamp_utc_time(dcl_controller_timestamp_str,
                                        DCL_CONTROLLER_TIMESTAMP_FORMAT)


def dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to NTP time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: NTP time (float64) in seconds and microseconds precision
    """

    utc_time = dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp_str)

    return float(ntplib.system_to_ntp_time(utc_time))


def mac_timestamp_to_utc_timestamp(mac_timestamp):
    """
    :param mac_timestamp: A mac based timestamp
    :return: The mac timestamp converted to unix time
    """

    unix_minus_mac_secs = (datetime(1970, 1, 1) - datetime(1904, 1, 1)).total_seconds()

    secs_since_1970 = mac_timestamp - unix_minus_mac_secs

    return secs_since_1970
