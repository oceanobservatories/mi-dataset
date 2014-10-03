__author__ = 'mworden'

import re


# A regex and matcher used to match any characters
ANY_CHARS_REGEX = r'.*'
ANY_CHARS_MATCHER = re.compile(ANY_CHARS_REGEX)

# A regex and matcher used to match a single space
SPACE_REGEX = ' '
SPACE_MATCHER = re.compile(SPACE_REGEX)

# A regex and matcher used to match the end of a line
END_OF_LINE_REGEX = r'(?:\r\n|\n)'
END_OF_LINE_MATCHER = re.compile(END_OF_LINE_REGEX)

# A regex and matcher used to match a float value
FLOAT_REGEX = r'(?:[+-]?[0-9]|[1-9][0-9])+\.[0-9]+'
FLOAT_MATCHER = re.compile(FLOAT_REGEX)

# A regex and matcher used to match an int value
INT_REGEX = r'[+-]?[0-9]+'
INT_MATCHER = re.compile(INT_REGEX)

# A regex and matcher used to match an unsigned int value
UNSIGNED_INT_REGEX = r'[0-9]+'
UNSIGNED_INT_MATCHER = re.compile(UNSIGNED_INT_REGEX)

# A regex and matcher used to match against one or more tab characters
MULTIPLE_TAB_REGEX = r'\t+'
MULTIPLE_TAB_MATCHER = re.compile(MULTIPLE_TAB_REGEX)

# A regex and matcher used to match against one or more whitespace characters
ONE_OR_MORE_WHITESPACE_REGEX = r'\s+'
ONE_OR_MORE_WHITESPACE_MATCHER = re.compile(ONE_OR_MORE_WHITESPACE_REGEX)

# A regex and matcher used to match a date in the format YYYY/MM/DD
DATE_YYYY_MM_DD_REGEX = r'(\d{4})/(\d{2})/(\d{2})'
DATE_YYYY_MM_DD_MATCHER = re.compile(DATE_YYYY_MM_DD_REGEX)

# A regex and matcher used to match time in the format of HH:MM:SS.mmm
TIME_HR_MIN_SEC_MSEC_REGEX = r'(\d{2}):(\d{2}):(\d{2})\.(\d{3})'
TIME_HR_MIN_SEC_MSEC_MATCHER = re.compile(TIME_HR_MIN_SEC_MSEC_REGEX)

# A regex and matcher used to match a date in the format MM/DD/YYYY
DATE_MM_DD_YYYY_REGEX = r'(\d{2})/(\d{2})/(\d{4})'
DATE_MM_DD_YYYY_MATCHER = re.compile(DATE_MM_DD_YYYY_REGEX)

# A regex and matcher used to match time in the format of HH:MM:SS
TIME_HR_MIN_SEC_REGEX = r'(\d{2}):(\d{2}):(\d{2})'
TIME_HR_MIN_SEC_MATCHER = re.compile(TIME_HR_MIN_SEC_REGEX)

