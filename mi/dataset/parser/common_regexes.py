__author__ = 'mworden'

# A regex used to match the end of a line
END_OF_LINE_REGEX = r'(?:\r\n|\n)'

# A regex used to match a float value
FLOAT_REGEX = r'(?:[+-]?[0-9]|[1-9][0-9])+\.[0-9]+'

# A regex used to match an int value
INT_REGEX = r'[+-]?[0-9]+'

# A regex used to match an unsigned int value
UNSIGNED_INT_REGEX = r'[0-9]+'

# A regex used to match against one or more tab characters
MULTIPLE_TAB_REGEX = r'\t+'

# A regex used to match against one or more whitespace characters
ONE_OR_MORE_WHITESPACE_REGEX = r'\s+'

# A regex to match ASCII-HEX characters
ASCII_HEX_CHAR_REGEX = r'[0-9A-Fa-f]'
