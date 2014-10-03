__author__ = 'mworden'

import re

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

# A regex to match ASCII-HEX characters
ASCII_HEX_CHAR_REGEX = r'[0-9A-Fa-f]'
ASCII_HEX_CHAR_MATCHER = re.compile(ASCII_HEX_CHAR_REGEX)
