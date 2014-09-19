__author__ = 'mworden'

import re

END_OF_LINE_REGEX = r'(?:\r\n|\n)'
END_OF_LINE_MATCHER = re.compile(r'.*' + END_OF_LINE_REGEX)

# A regex to capture a float value
FLOAT_REGEX = r'(?:[+-]?[0-9]|[1-9][0-9])+\.[0-9]+'
FLOAT_MATCHER = re.compile(FLOAT_REGEX)

# A regex to capture an int value
INT_REGEX = r'[+-]?[0-9]+'
INT_MATCHER = re.compile(INT_REGEX)

# A regex to capture an unsigned int value
UNSIGNED_INT_REGEX = r'[0-9]+'
UNSIGNED_INT_MATCHER = re.compile(UNSIGNED_INT_REGEX)

# A regex to match against one or more tab characters
MULTIPLE_TAB_REGEX = r'\t+'
MULTIPLE_TAB_MATCHER = re.compile(MULTIPLE_TAB_REGEX)

ONE_OR_MORE_WHITESPACE_REGEX = r'\s+'
ONE_OR_MORE_WHITESPACE_MATCHER = re.compile(ONE_OR_MORE_WHITESPACE_REGEX)
