#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdbp_cdef_dcl_ce
@file marine-integrations/mi/dataset/parser/ctdbp_cdef_dcl_ce.py
@author Christopher Fortin
@brief Parser for the ctdbp_cdef_dcl_ce dataset driver

This file contains code for the ctdbp_cdef_dcl_ce parsers and code to produce data particles.
For telemetered data, there is one parser which produces two types of data particles.
For recovered data, there is one parser which produces two types of data particles.
The input file formats are the same for both recovered and telemetered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 2 types of records.
The record types are separated by a newline.
All lines start with a timestamp.
Metadata records: timestamp [text] more text newline.
Instrument records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

There are two file formats supported, the current config, which is lusted as incorrect,
and the subsequent files after the inst config is corrected, labelled here as UNCORRected
and CORRected.

Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import re

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
UINT = r'(\d*)'                      # unsigned integer as a group
COMMA = ','                          # simple comma
COLON = ':'                          # simple colon
HASH = '#'                           # hash symbol
START_GROUP = '('                    # match group start
END_GROUP = ')'                      # match group end
FLOAT = START_GROUP + FLOAT_REGEX + END_GROUP   # float capture
ZERO_OR_MORE_WHITESPACE_REGEX = r'\s*'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
DATE_TIME_STR = r'(\d{2} \D{3} \d{4} \d{2}:\d{2}:\d{2})'              # DateTimeStr:   DD Mon YYYY HH:MM:SS
TIMESTAMP = START_GROUP
TIMESTAMP += DATE_YYYY_MM_DD_REGEX + ONE_OR_MORE_WHITESPACE_REGEX
TIMESTAMP += TIME_HR_MIN_SEC_MSEC_REGEX
TIMESTAMP += END_GROUP                                                # put together the BOL timestamp
START_METADATA = r'\['                                                # metadata delimited by []'s
END_METADATA = r'\]'

# All presf records are ASCII characters separated by a newline.
CTDBP_RECORD_PATTERN = ANY_CHARS_REGEX       # Any number of ASCII characters
CTDBP_RECORD_PATTERN += END_OF_LINE_REGEX       # separated by a new line
CTDBP_RECORD_MATCHER = re.compile(CTDBP_RECORD_PATTERN)

# LOGGER IDENTIFICATION, such as [ctbp1:DLOGP3]
LOGGERID_PATTERN = START_METADATA                                   # Metadata record starts with '['
LOGGERID_PATTERN += ANY_CHARS_REGEX                                 # followed by text
LOGGERID_PATTERN += END_METADATA                                    # followed by ']'
LOGGERID_PATTERN += COLON                                           # an immediate colon
LOGGERID_PATTERN += ZERO_OR_MORE_WHITESPACE_REGEX                   # and maybe whitespace after

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX          # dcl controller timestamp
METADATA_PATTERN += LOGGERID_PATTERN                                 # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX                                  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX                                # metadata record ends newline
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# match a single line uncorrected instrument record
UNCORR_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX + HASH        # dcl controller timestamp
TIMESTAMP_MATCHER = re.compile(UNCORR_REGEX)                          # a common matcher for the BOL timestamp
UNCORR_REGEX += ONE_OR_MORE_WHITESPACE_REGEX + FLOAT                  # temp
UNCORR_REGEX += COMMA + ONE_OR_MORE_WHITESPACE_REGEX               
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # conductivity
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # pressure
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # salinity
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # sound vel
UNCORR_REGEX += DATE_TIME_STR + COMMA + ONE_OR_MORE_WHITESPACE_REGEX    # date time
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # sigma t
UNCORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX          # sigma v
UNCORR_REGEX += FLOAT + ONE_OR_MORE_WHITESPACE_REGEX                  # sigma I
UNCORR_REGEX += END_OF_LINE_REGEX
UNCORR_MATCHER = re.compile(UNCORR_REGEX)

# match a single line corrected instrument record
CORR_REGEX = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX                    # dcl controller timestamp
CORR_REGEX += '(?:' + HASH + '|' + LOGGERID_PATTERN + ')'                # a loggerid or a hash
CORR_REGEX += ZERO_OR_MORE_WHITESPACE_REGEX
CORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX               # temp
CORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX                # conductivity
CORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX               # pressure
CORR_REGEX += FLOAT + COMMA + ONE_OR_MORE_WHITESPACE_REGEX               # optode oxygen
CORR_REGEX += DATE_TIME_STR + ONE_OR_MORE_WHITESPACE_REGEX               # date time
CORR_REGEX += END_OF_LINE_REGEX
CORR_MATCHER = re.compile(CORR_REGEX)


# UNCORR_MATCHER produces the following groups:
UNCORR_GROUP_DCL_TIMESTAMP = 1
UNCORR_GROUP_DATE_TIME_STRING = 14
UNCORR_GROUP_TEMPERATURE = 9
UNCORR_GROUP_CONDUCTIVITY = 10
UNCORR_GROUP_PRESSURE = 11
NUM_UNCORR_MATCHGROUPS = 17

# CORR_MATCHER produces the following groups:
CORR_GROUP_DCL_TIMESTAMP = 1
CORR_GROUP_DATE_TIME_STRING = 13
CORR_GROUP_OPTODE = 12
CORR_GROUP_TEMPERATURE = 9
CORR_GROUP_CONDUCTIVITY = 10
CORR_GROUP_PRESSURE = 11
NUM_CORR_MATCHGROUPS = 13

# This table is used in the generation of the tide data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
DATA_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    UNCORR_GROUP_DCL_TIMESTAMP,            str),
    ('temp',                        UNCORR_GROUP_TEMPERATURE,              float),
    ('conductivity',                UNCORR_GROUP_CONDUCTIVITY,             float),
    ('pressure',                    UNCORR_GROUP_PRESSURE,                 float),
    ('date_time_string',            UNCORR_GROUP_DATE_TIME_STRING,         str)
]

CORRDATA_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    CORR_GROUP_DCL_TIMESTAMP,            str),
    ('temp',                        CORR_GROUP_TEMPERATURE,              float),
    ('conductivity',                CORR_GROUP_CONDUCTIVITY,             float),
    ('pressure',                    CORR_GROUP_PRESSURE,                 float),
    ('date_time_string',            CORR_GROUP_DATE_TIME_STRING,         str)
]

# This table is used in the generation of the tide data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
DOSTA_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    CORR_GROUP_DCL_TIMESTAMP,            str),
    ('dosta_ln_optode_oxygen',      CORR_GROUP_OPTODE,                   float),
    ('date_time_string',            CORR_GROUP_DATE_TIME_STRING,         str)
]

# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the ctdbp particle class
PARTICLE_CLASS_KEY = 'particle_class'
DOSTA_CLASS_KEY = 'dosta_class'


class DataParticleType(BaseEnum):
    INSTRUMENT_TELEMETERED = 'ctdbp_cdef_dcl_ce_instrument'
    INSTRUMENT_RECOVERED = 'ctdbp_cdef_dcl_ce_instrument_recovered'
    DOSTA_TELEMETERED = 'ctdbp_cdef_dcl_ce_dosta'
    DOSTA_RECOVERED = 'ctdbp_cdef_dcl_ce_dosta_recovered'


class CtdbpCdefDclCeParserDataParticle(DataParticle):
    """
    Class for parsing data from the data set
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CtdbpCdefDclCeParserDataParticle, self).__init__(raw_data,
                                                               port_timestamp,
                                                               internal_timestamp,
                                                               preferred_timestamp,
                                                               quality_flag,
                                                               new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        # This works for both the uncorrected and the corrected files
        utc_time = dcl_controller_timestamp_to_utc_time(self.raw_data.group(1))
        self.set_internal_timestamp(unix_time=utc_time)
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """       

        # set map based upon whether this is a corrected or unsecorrected record
        num_matches = len(self.raw_data.groups())
        if num_matches == NUM_CORR_MATCHGROUPS:
            return [self._encode_value(name, self.raw_data.group(group), function)
                    for name, group, function in CORRDATA_PARTICLE_MAP]
        elif num_matches == NUM_UNCORR_MATCHGROUPS:
            return [self._encode_value(name, self.raw_data.group(group), function)
                    for name, group, function in DATA_PARTICLE_MAP]


class CtdbpCdefDclCeParserDostaParticle(DataParticle):
    """
    Class for parsing data from the data set
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CtdbpCdefDclCeParserDostaParticle, self).__init__(raw_data,
                                                                port_timestamp,
                                                                internal_timestamp,
                                                                preferred_timestamp,
                                                                quality_flag,
                                                                new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        # This works for both the uncorrected and the corrected files

        utc_time = dcl_controller_timestamp_to_utc_time(self.raw_data.group(1))
        self.set_internal_timestamp(unix_time=utc_time)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """
        
        return [self._encode_value(name, self.raw_data.group(group), function)
                for name, group, function in DOSTA_PARTICLE_MAP]


class CtdbpCdefDclCeRecoveredParserDataParticle(CtdbpCdefDclCeParserDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED


class CtdbpCdefDclCeTelemeteredParserDataParticle(CtdbpCdefDclCeParserDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED


class CtdbpCdefDclCeRecoveredParserDostaParticle(CtdbpCdefDclCeParserDostaParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.DOSTA_RECOVERED


class CtdbpCdefDclCeTelemeteredParserDostaParticle(CtdbpCdefDclCeParserDostaParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.DOSTA_TELEMETERED


class CtdbpCdefDclCeParser(SimpleParser):
    """
    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        super(CtdbpCdefDclCeParser, self).__init__(config,
                                                   stream_handle,
                                                   exception_callback)

        self.input_file = stream_handle

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            # Set the particle classes to be used later
            if PARTICLE_CLASS_KEY in particle_classes_dict:
                self._particle_class = particle_classes_dict.get(PARTICLE_CLASS_KEY)
            if DOSTA_CLASS_KEY in particle_classes_dict:
                self._dosta_class = particle_classes_dict.get(DOSTA_CLASS_KEY)
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
 
    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """
        
        # read the first line in the file
        line = self._stream_handle.readline()

        while line:            
            # a instrument message, also a single line
            test_uncorr = UNCORR_MATCHER.match(line)
            # a corrected instrument message, also a single line
            test_corr = CORR_MATCHER.match(line)

            if test_uncorr is not None:
                log.debug('uncorr record found')
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     test_uncorr,
                                                     None)
                self._record_buffer.append(data_particle)

            elif test_corr is not None:
                log.debug('corr record found')
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     test_corr,
                                                     None)
                self._record_buffer.append(data_particle)
                
                data_particle = self._extract_sample(self._dosta_class,
                                                     None,
                                                     test_corr,
                                                     None)
                self._record_buffer.append(data_particle)

            else:
                # NOTE: Need to check for the metadata line last, since the CORR group also has the [*] pattern
                # check for a metadata line ( a single line )
                test_meta = METADATA_MATCHER.match(line)
                
                if test_meta is None:
                    # something in the data didn't match a required regex, so raise an exception and press on.
                    message = "Error while decoding parameters in data: [%s]" % line
                    self._exception_callback(RecoverableSampleException(message))
           
            # read the next line in the file
            line = self._stream_handle.readline()

