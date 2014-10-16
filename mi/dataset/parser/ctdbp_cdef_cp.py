#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdbp_cdef_cp
@file marine-integrations/mi/dataset/parser/ctdbp_cdef_cp.py
@author Tapana Gupta
@brief Parser for the ctdbp_cdef_cp dataset driver

This file contains code for the ctdbp_cdef_cp parser and code to produce data
particles. This parser is for recovered data only - it produces a single
particle for the data recovered from the instrument.

The input file is ASCII. There are two sections of data contained in the
input file.  The first is a set of header information, and the second is a set
of hex ascii data with one data sample per line in the file. Each line in the
header section starts with a '*'. The header lines are simply ignored.
Each line of sample data produces a single data particle.
Malformed sensor data records and all header records produce no particles.

Release notes:

Initial Release
"""

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'


import copy
import calendar
from functools import partial
import re

from mi.core.instrument.chunker import \
    StringChunker

from mi.core.log import get_logger; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.exceptions import \
    SampleException, \
    UnexpectedDataException

from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey

from mi.dataset.dataset_parser import BufferLoadingParser

from mi.dataset.parser.common_regexes import \
    END_OF_LINE_REGEX

from ctdbp_common import CtdbpParserDataParticleKey, \
    HEADER_MATCHER, \
    CTDBP_RECORD_MATCHER, \
    JAN_1_2000

# Basic patterns

# Regex for identifying a single record of instrument data (30 Hex characters)
HEX_DATA_30 = r'([0-9A-F]{30})'

# Regex for data particle class
# Each data record is in the following format:
# ttttttccccccppppppvvvvssssssss
# where each character indicates one hex ascii character.
# First 6 chars: tttttt = Temperature A/D counts
# Next 6 chars: cccccc = Conductivity A/D counts
# Next 6 chars: pppppp = pressure A/D counts
# Next 4 chars: vvvv = temperature compensation A/D counts
# Last 8 chars: ssssssss = seconds since January 1, 2000
DATA_REGEX = r"""
(?P<temp>       [0-9A-F]{6})
(?P<cond>       [0-9A-F]{6})
(?P<press>      [0-9A-F]{6})
(?P<press_temp> [0-9A-F]{4})
(?P<ctd_time>   [0-9A-F]{8})"""

DATA_MATCHER = re.compile(DATA_REGEX, re.VERBOSE|re.DOTALL)

# Sensor data record:
#  (30 Hex Characters followed by new line)
SENSOR_DATA_PATTERN = HEX_DATA_30 + END_OF_LINE_REGEX
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)


class DataParticleType(BaseEnum):
    """
    Class that defines the data particle generated from the ctdbp_cdef_cp recovered data
    """
    SAMPLE = 'ctdbp_cdef_cp_instrument_recovered'


class CtdbpCdefCpInstrumentDataParticle(DataParticle):
    """
    Class for generating the ctdbp_cdef_cp_instrument_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take recovered Hex raw data and extract different fields, converting Hex to Integer values.
        @throws SampleException If there is a problem with sample creation
        """

        # the data contains seconds since Jan 1, 2000. Need the number of seconds before that
        SECONDS_TILL_JAN_1_2000 = calendar.timegm(JAN_1_2000)

        match = DATA_MATCHER.match(self.raw_data)
        if not match:
            raise SampleException("CtdbpCdefCpInstrumentDataParticle: No regex match of \
                                  parsed sample data: [%s]", self.raw_data)
        try:
            # grab Hex values, convert to int
            temp = int(match.group('temp'), 16)
            cond = int(match.group('cond'), 16)
            press = int(match.group('press'), 16)
            press_temp = int(match.group('press_temp'), 16)
            ctd_time = int(match.group('ctd_time'), 16)

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data))

        # calculate the internal timestamp
        elapsed_seconds = SECONDS_TILL_JAN_1_2000 + ctd_time
        self.set_internal_timestamp(unix_time=elapsed_seconds)

        result = [{DataParticleKey.VALUE_ID: CtdbpParserDataParticleKey.TEMPERATURE,
                   DataParticleKey.VALUE: temp},
                  {DataParticleKey.VALUE_ID: CtdbpParserDataParticleKey.CONDUCTIVITY,
                   DataParticleKey.VALUE: cond},
                  {DataParticleKey.VALUE_ID: CtdbpParserDataParticleKey.PRESSURE,
                   DataParticleKey.VALUE: press},
                  {DataParticleKey.VALUE_ID: CtdbpParserDataParticleKey.PRESSURE_TEMP,
                   DataParticleKey.VALUE: press_temp},
                  {DataParticleKey.VALUE_ID: CtdbpParserDataParticleKey.CTD_TIME,
                   DataParticleKey.VALUE: ctd_time}]
        log.debug('CtdbpCdefCpInstrumentDataParticle: particle=%s', result)

        return result


class CtdbpCdefCpParser(BufferLoadingParser):

    """
    Parser for CTDBP_cdef_cp data.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        # No fancy sieve function needed for this parser.
        # File is ASCII with records separated by newlines.

        super(CtdbpCdefCpParser, self).__init__(config,
                                          stream_handle,
                                          None,
                                          partial(StringChunker.regex_sieve_function,
                                                  regex_list=[CTDBP_RECORD_MATCHER]),
                                          state_callback,
                                          publish_callback,
                                          exception_callback,
                                          *args,
                                          **kwargs)

        self.input_file = stream_handle


    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:

            self._exception_callback(UnexpectedDataException(
                "Found %d bytes of un-expected non-data %s" %
                (len(non_data), non_data)))


    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:

            # If this is a valid sensor data record,
            # use the extracted fields to generate a particle.

            sensor_match = SENSOR_DATA_MATCHER.match(chunk)

            if sensor_match is not None:

                particle = self._extract_sample(CtdbpCdefCpInstrumentDataParticle,
                                                None,
                                                chunk,
                                                None)

                if particle is not None:
                    result_particles.append((particle, None))

            # It's not a sensor data record, see if it's a header record.

            else:

                # If it's a valid header record, ignore it.
                # Otherwise generate warning for unknown data.

                header_match = HEADER_MATCHER.match(chunk)

                log.debug("Header match: %s", str(header_match))
                if header_match is None:
                    error_message = 'Unknown data found in chunk %s' % chunk
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles
