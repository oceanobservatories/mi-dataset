#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/winch_cspp.py
@author Richard Han
@brief Parser for the Winch CSPP dataset driver
Release notes:

Initial Release
"""
import re
from mi.dataset.dataset_parser import SimpleParser

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'


import calendar
import ntplib

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, ONE_OR_MORE_WHITESPACE_REGEX, INT_REGEX


# A regex to match a Winch's word including an underscore character
WORD_REGEX = r'[A-Z_a-z]+'

# Regex to match Winch's date and time
DATE_YYYY_MM_DD_REGEX = r'\d{4}-\d{2}-\d{2}'
TIME_HR_MIN_SEC_MSEC_REGEX = r'\d{2}:\d{2}:\d{2}\.\d{3}'

ANY_CHARS_EXCEPT_CRLF_REGEX = r'[^\r\n]*'
ZERO_OR_ONE_COMMAS_REGEX = r'\,?'
COMMAS_REGEX = r'\,'


class DataParticleType(BaseEnum):
    """
    The data particle types that this parser can generate
    """
    WINCH_CSPP = 'winch_cspp'


class WinchCsppParserDataParticleKey(BaseEnum):
    """
    The data particle keys associated with Winch CSPP data particle parameters
    """
    DATE = 'date'
    TIME = 'time'
    WINCH_STATE = 'winch_state'
    WINCH_SPEED = 'winch_speed'
    WINCH_PAYOUT = 'winch_payout'
    WINCH_CURRENT_DRAW = 'winch_current_draw'
    SENSOR_CURRENT_DRAW = 'sensor_current_draw'
    WINCH_STATUS = 'winch_status'


# Basic patterns
common_matches = {
    'comma' : COMMAS_REGEX,
    'zero_or_one_comma' : ZERO_OR_ONE_COMMAS_REGEX,
    'date': DATE_YYYY_MM_DD_REGEX,
    'time': TIME_HR_MIN_SEC_MSEC_REGEX,
    'word': WORD_REGEX,
    'int': INT_REGEX,
    'float': FLOAT_REGEX,
    'any_chars_except_crlf': ANY_CHARS_EXCEPT_CRLF_REGEX,
    'end_of_line' :  END_OF_LINE_REGEX,
    'one_or_more_whitespace' : ONE_OR_MORE_WHITESPACE_REGEX,

}

# Add together the particle keys and the common matches dictionaries
# for use as variables in the regexes defined below.
common_matches.update(WinchCsppParserDataParticleKey.__dict__)

# Regex used to parse Winch SCPP data
WINCH_DATA_MATCHER = re.compile(r"""(?x)
    (?P<%(DATE)s>                 %(date)s)   %(one_or_more_whitespace)s
    (?P<%(TIME)s>                 %(time)s)   %(one_or_more_whitespace)s
    (?P<%(WINCH_STATE)s>          %(word)s)   %(comma)s
    (?P<%(WINCH_SPEED)s>          %(int)s)    %(comma)s
    (?P<%(WINCH_PAYOUT)s>         %(int)s)    %(comma)s
    (?P<%(WINCH_CURRENT_DRAW)s>   %(float)s)  %(comma)s
    (?P<%(SENSOR_CURRENT_DRAW)s>  %(float)s)  %(zero_or_one_comma)s
    (?P<%(WINCH_STATUS)s>         %(any_chars_except_crlf)s)
    %(end_of_line)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# A group of Winch CSPP particle encoding rules used to simplify encoding using a loop
WINCH_CSPP_PARTICLE_ENCODING_RULES = [
    (WinchCsppParserDataParticleKey.DATE, str),
    (WinchCsppParserDataParticleKey.TIME, str),
    (WinchCsppParserDataParticleKey.WINCH_STATE, str),
    (WinchCsppParserDataParticleKey.WINCH_SPEED, int),
    (WinchCsppParserDataParticleKey.WINCH_PAYOUT, int),
    (WinchCsppParserDataParticleKey.WINCH_CURRENT_DRAW, float),
    (WinchCsppParserDataParticleKey.SENSOR_CURRENT_DRAW, float),
    (WinchCsppParserDataParticleKey.WINCH_STATUS, str)
]


class WinchCsppDataParticle(DataParticle):
    """
    Class for generating a Winch CSPP data particle
    """
    _data_particle_type = DataParticleType.WINCH_CSPP


    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws RecoverableSampleException If there is a problem with sample creation
        """

        try:

            # Generate a particle by calling encode_value for each entry
            # in the Instrument Particle Mapping table,
            # where each entry is a tuple containing the particle field name
            # and a function to use for data conversion.

            return [self._encode_value(name, self.raw_data[name], function)
                    for name, function in WINCH_CSPP_PARTICLE_ENCODING_RULES]

        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Exception when building parsed values")
            raise RecoverableSampleException(
                "Error (%s) while encoding parameters in data: [%s]"
                % (ex, self.raw_data))


class WinchCsppParser(SimpleParser):
    """
    Parser for Winch CSPP data.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        """
        This method is a constructor that will instantiate an WinchCsppParser object.
        @param config The configuration for this WinchCsppParser parser
        @param stream_handle The handle to the data stream containing the Winch cspp data
        @param exception_callback The function to call to report exceptions
        """
        super(WinchCsppParser, self).__init__(config, stream_handle, exception_callback)


    def parse_file(self):
        """
        Parse Winch CSPP text file.
        """

        # loop over all lines in the data file and parse the data to generate Winch CSPP particles
        line = self._stream_handle.readline()
        while line:

            match = WINCH_DATA_MATCHER.match(line)
            if match:
                winch_data_dict = {}

                date = match.group(WinchCsppParserDataParticleKey.DATE)
                year, month, day = date.split('-')
                hour, minute, second =  match.group(WinchCsppParserDataParticleKey.TIME).split(':')
                second, hundred_sec = second.split('.')

                unix_time = calendar.timegm((int(year), int(month), int(day), int(hour), int(minute), int(second)+int(hundred_sec)/1e3))
                time_stamp = ntplib.system_to_ntp_time(unix_time)

                winch_data_dict[WinchCsppParserDataParticleKey.DATE] = \
                    match.group(WinchCsppParserDataParticleKey.DATE)
                winch_data_dict[WinchCsppParserDataParticleKey.TIME] = \
                    match.group(WinchCsppParserDataParticleKey.TIME)
                winch_data_dict[WinchCsppParserDataParticleKey.WINCH_STATE] = \
                    match.group(WinchCsppParserDataParticleKey.WINCH_STATE)
                winch_data_dict[WinchCsppParserDataParticleKey.WINCH_SPEED] = \
                    match.group(WinchCsppParserDataParticleKey.WINCH_SPEED)
                winch_data_dict[WinchCsppParserDataParticleKey.WINCH_PAYOUT] = \
                    match.group(WinchCsppParserDataParticleKey.WINCH_PAYOUT)
                winch_data_dict[WinchCsppParserDataParticleKey.WINCH_CURRENT_DRAW] = \
                    match.group(WinchCsppParserDataParticleKey.WINCH_CURRENT_DRAW)
                winch_data_dict[WinchCsppParserDataParticleKey.SENSOR_CURRENT_DRAW] = \
                    match.group(WinchCsppParserDataParticleKey.SENSOR_CURRENT_DRAW)
                winch_data_dict[WinchCsppParserDataParticleKey.WINCH_STATUS] = \
                    match.group(WinchCsppParserDataParticleKey.WINCH_STATUS)

                # Generate a Winch CSPP particle using the winch_data_dict and add it to the internal buffer
                particle = self._extract_sample(WinchCsppDataParticle, None, winch_data_dict, time_stamp)
                if particle is not None:
                    self._record_buffer.append(particle)

            else:
                 # If it is not a valid Winch Cspp record, ignore it.
                 error_message = 'Winch Cspp data regex does not match for line: %s' % line
                 log.warn(error_message)
                 self._exception_callback(RecoverableSampleException(error_message))

            # read the next line in the file
            line = self._stream_handle.readline()
