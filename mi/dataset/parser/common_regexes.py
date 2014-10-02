__author__ = 'mworden'


# A regex used to match any characters
ANY_CHARS_REGEX = r'.*'

# A regex used to match a single space
SPACE_REGEX = ' '

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

# A regex used to match a date in the format YYYY/MM/DD
DATE_YYYY_MM_DD_REGEX = r'(\d{4})/(\d{2})/(\d{2})'

# A regex used to match time in the format of HH:MM:SS.mmm
TIME_HR_MIN_SEC_MSEC_REGEX = r'(\d{2}):(\d{2}):(\d{2})\.(\d{3})'

# A regex used to match a date in the format MM/DD/YYYY
DATE_MM_DD_YYYY_REGEX = r'(\d{2})/(\d{2})/(\d{4})'

# A regex used to match time in the format of HH:MM:SS
TIME_HR_MIN_SEC_REGEX = r'(\d{2}):(\d{2}):(\d{2})'
