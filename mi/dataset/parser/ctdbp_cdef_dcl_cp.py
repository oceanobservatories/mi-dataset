#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdbp_cdef_dcl_cp
@file marine-integrations/mi/dataset/parser/ctdbp_cdef_dcl_cp.py
@author Christopher Fortin
@brief Parser for the ctdbp_cdef_dcl_cp dataset driver

This file contains code for the ctdbp_cdef_dcl_cp parsers and code to produce data particles.
For telemetered data, there is one parser which produces two types of data particles.
For recovered data, there is one parser which produces two types of data particles.
The input file formats are the same for both recovered and telemetered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 2 types of records.
The record types are separated by a newline.
All lines start with a timestamp.
Status records: timestamp [text] more text newline.
Instrument records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all status records produce no particles.

The data records from this device could be in one of two different formats, depending
upon whether the system configuration has been corrected.

From the IDD 
Each valid instrument data record in a ctdbp_cdef_dcl_cp data file will look like one of the the following:
yyyy/mm/dd hh:mm:ss.sss # aa.aaaa, b.bbbbb, c.ccc, dd.dddd, eeee.eee, DD MMM YYYY HH:MM:SS, ff.ffff, gg.g, h.h
or
yyyy/mm/dd hh:mm:ss.sss # aa.aaaa, b.bbbbb, c.ccc, DD MMM YYYY HH:MM:SS

There are two file formats supported, the current config, which is lusted as incorrect,
and the subsequent files after the inst config is corrected, labelled here as UNCORRected
and CORRected.

Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import calendar
import re
from datetime import datetime
import time

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.core.exceptions import \
    RecoverableSampleException, \
    ConfigurationException

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time

from mi.dataset.parser.common_regexes import \
    ANY_CHARS_REGEX, \
    END_OF_LINE_REGEX, \
    FLOAT_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    TIME_HR_MIN_SEC_MSEC_REGEX, \
    DATE_YYYY_MM_DD_REGEX

# Basic patterns
COMMA = ','                          # simple comma
HASH = '#'                           # hash symbol
TAB = '\t'                           # simple tab
START_GROUP = '('                    # match group start
END_GROUP = ')'                      # match group end
#FLOAT = r'([1-9][0-9]*\.?[0-9]*)'
FLOAT = START_GROUP + FLOAT_REGEX + END_GROUP

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Status fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
DATE_TIME_STR = r'(\d{2} \D{3} \d{4} \d{2}:\d{2}:\d{2})'                      # DateTimeStr:   DD Mon YYYY HH:MM:SS
TIMESTAMP = START_GROUP + DATE_YYYY_MM_DD_REGEX
TIMESTAMP += ONE_OR_MORE_WHITESPACE_REGEX
TIMESTAMP += TIME_HR_MIN_SEC_MSEC_REGEX + END_GROUP                           # put together the BOL timestamp
START_STATUS = r'\['                                                          # status delimited by []'s
END_STATUS = r'\]'

# All presf records are ASCII characters separated by a newline.
CTDBP_RECORD_PATTERN = ANY_CHARS_REGEX            # Any number of ASCII characters
CTDBP_RECORD_PATTERN += END_OF_LINE_REGEX         # separated by a new line
CTDBP_RECORD_MATCHER = re.compile(CTDBP_RECORD_PATTERN)

# Status record:
#   Timestamp [Text]MoreText newline
STATUS_PATTERN = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX                           # dcl controller timestamp
STATUS_PATTERN += START_STATUS                                                      # Status record starts with '['
STATUS_PATTERN += ANY_CHARS_REGEX                                                   # followed by text
STATUS_PATTERN += END_STATUS                                                        # followed by ']'
STATUS_PATTERN += ANY_CHARS_REGEX                                                   # followed by more text
STATUS_PATTERN += END_OF_LINE_REGEX                                                 # status record ends newline
STATUS_MATCHER = re.compile(STATUS_PATTERN)

# match a single line uncorrected instrument record
UNCORR_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + HASH                # dcl controller timestamp
TIMESTAMP_MATCHER = re.compile(UNCORR_REGEX)                                  # a common matcher for the BOL timestamp
UNCORR_REGEX += ONE_OR_MORE_WHITESPACE_REGEX + FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX   # temp
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                  # conductivity
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                  # pressure
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                  # salinity
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                  # sound vel
UNCORR_REGEX += DATE_TIME_STR + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                # date time
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                  # sigma t
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                  # sigma v
UNCORR_REGEX += FLOAT + ONE_OR_MORE_WHITESPACE_REGEX                                          # sigma I
UNCORR_REGEX += END_OF_LINE_REGEX
UNCORR_MATCHER = re.compile(UNCORR_REGEX)

# match a single line corrected instrument record
CORR_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + HASH                              # dcl controller timestamp
CORR_REGEX += ONE_OR_MORE_WHITESPACE_REGEX + FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX # temp
CORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                # conductivity
CORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                                # pressure
CORR_REGEX += DATE_TIME_STR + END_OF_LINE_REGEX                                           # date time
CORR_MATCHER = re.compile(CORR_REGEX)


# UNCORR_MATCHER produces the following groups:
UNCORR_GROUP_DCL_TIMESTAMP = 1
UNCORR_GROUP_DATE_TIME_STRING = 14
UNCORR_GROUP_TEMPERATURE = 9
UNCORR_GROUP_CONDUCTIVITY = 10
UNCORR_GROUP_PRESSURE = 11

# CORR_MATCHER produces the following groups:
CORR_GROUP_DCL_TIMESTAMP = 1
CORR_GROUP_DATE_TIME_STRING = 12
CORR_GROUP_TEMPERATURE = 9
CORR_GROUP_CONDUCTIVITY = 10
CORR_GROUP_PRESSURE = 11

# This table is used in the generation of the tide data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
UNCORR_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    UNCORR_GROUP_DCL_TIMESTAMP,            str),
    ('temp',                        UNCORR_GROUP_TEMPERATURE,              float),
    ('conductivity',                UNCORR_GROUP_CONDUCTIVITY,             float),
    ('pressure',                    UNCORR_GROUP_PRESSURE,                 float),
    ('date_time_string',            UNCORR_GROUP_DATE_TIME_STRING,         str)
]

# This table is used in the generation of the tide data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
CORR_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    CORR_GROUP_DCL_TIMESTAMP,            str),
    ('temp',                        CORR_GROUP_TEMPERATURE,              float),
    ('conductivity',                CORR_GROUP_CONDUCTIVITY,             float),
    ('pressure',                    CORR_GROUP_PRESSURE,                 float),
    ('date_time_string',            CORR_GROUP_DATE_TIME_STRING,         str)
]


# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the ctdbp particle class
PARTICLE_CLASS_KEY = 'particle_class'


class DataParticleType(BaseEnum):
    INSTRUMENT_TELEMETERED = 'ctdbp_cdef_dcl_cp_instrument'
    INSTRUMENT_RECOVERED = 'ctdbp_cdef_dcl_cp_instrument_recovered'


class CtdbpCdefDclCpParserDataParticle(DataParticle):
    """
    Class for parsing data from the data set
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CtdbpCdefDclCpParserDataParticle, self).__init__(raw_data,
                                                                port_timestamp,
                                                                internal_timestamp,
                                                                preferred_timestamp,
                                                                quality_flag,
                                                                new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        # This works for both the uncorrected and the corrected files
 
        utc_time = dcl_controller_timestamp_to_utc_time(raw_data.group(1))
        self.set_internal_timestamp(unix_time=utc_time)
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """
        numMatched = len(self.raw_data.groups())
                
        # uncorrected has 17 match groups.        
        if numMatched is 17:
            log.debug('parsing uncorrected particle')
            return [self._encode_value(name, self.raw_data.group(group), function)
                    for name, group, function in UNCORR_PARTICLE_MAP]
        else:
            log.debug('parsing corrected particle')
            return [self._encode_value(name, self.raw_data.group(group), function)
                    for name, group, function in CORR_PARTICLE_MAP]



class CtdbpCdefDclCpRecoveredParserDataParticle(CtdbpCdefDclCpParserDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED


class CtdbpCdefDclCpTelemeteredParserDataParticle(CtdbpCdefDclCpParserDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED




class CtdbpCdefDclCpParser(SimpleParser):
    """
    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 *args, **kwargs):

        super(CtdbpCdefDclCpParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback)

        self._particle_class = config.get(DataSetDriverConfigKeys.PARTICLE_CLASS)
        
    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        # read the first line in the file
        line = self._stream_handle.readline()


        while line:
 
            test_inst = UNCORR_MATCHER.match(line)
            test_meta = STATUS_MATCHER.match(line)
            test_corr = CORR_MATCHER.match(line)
            
            if test_meta is not None:
                log.debug("meta match")

            elif test_inst is not None:
                log.debug("uncorrected sample match")
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     test_inst,
                                                     None)
                self._record_buffer.append(data_particle)

            # a corrected instrument message, also a single line
            elif test_corr is not None:
                log.debug("corrected sample match")
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     test_corr,
                                                     None)
                self._record_buffer.append(data_particle)
           
            else:
                # received an unrecognized line
                log.warn("Exception when parsing file")
                message = "Error while parsing line: [%s]" + line
                self._exception_callback(RecoverableSampleException(message))

            # read the next line in the file
            line = self._stream_handle.readline()

