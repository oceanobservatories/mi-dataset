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

from datetime import datetime
import ntplib
import time

from mi.core.log import get_logger
log = get_logger()

# Format of DCL Controller Timestamp in records
# Example: 2014/08/17 00:57:10.648
DCL_CONTROLLER_TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S.%f"


def dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to UTC time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: UTC time in seconds and microseconds precision
    """

    # Convert the dcl controller timestamp string into a datetime object, which
    # represents time as a series of attributes (year, month, day, hour,
    # minute, second, microsecond).
    dcl_datetime = datetime.strptime(dcl_controller_timestamp_str, DCL_CONTROLLER_TIMESTAMP_FORMAT)

    # strftime converts the datetime object back into a string of seconds
    # (and microseconds) since 1970 in local time.  The float() then converts
    # the floating point string into a float number.  The time.timezone is a
    # floating point number representing an offset from utc in seconds for the
    # local time.  The timezone offset needs to be subtracted to take out the
    # seconds which were included in the dcl_datetime.strftime("%s.%f")
    # conversion to make it in utc time.

    return float(dcl_datetime.strftime("%s.%f")) - time.timezone


def dcl_controller_timestamp_to_ntp_time(dcl_controller_timestamp_str):
    """
    Converts a DCL controller timestamp string to NTP time.
    :param dcl_controller_timestamp_str: a DCL controller timestamp string
    :return: NTP time (float64) in seconds and microseconds precision
    """

    utc_time = dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp_str)

    return float(ntplib.system_to_ntp_time(utc_time))
