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
from mi.dataset.dataset_parser import BufferLoadingParser

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
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
DATE_TIME_STR = r'(\d{2} \D{3} \d{4} \d{2}:\d{2}:\d{2})'                      # DateTimeStr:   DD Mon YYYY HH:MM:SS
TIMESTAMP = START_GROUP + DATE_YYYY_MM_DD_REGEX
TIMESTAMP += ONE_OR_MORE_WHITESPACE_REGEX
TIMESTAMP += TIME_HR_MIN_SEC_MSEC_REGEX + END_GROUP                           # put together the BOL timestamp
START_METADATA = r'\['                                                        # metadata delimited by []'s
END_METADATA = r'\]'

# All presf records are ASCII characters separated by a newline.
CTDBP_RECORD_PATTERN = ANY_CHARS_REGEX            # Any number of ASCII characters
CTDBP_RECORD_PATTERN += END_OF_LINE_REGEX         # separated by a new line
CTDBP_RECORD_MATCHER = re.compile(CTDBP_RECORD_PATTERN)

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + ONE_OR_MORE_WHITESPACE_REGEX                           # dcl controller timestamp
METADATA_PATTERN += START_METADATA                                                    # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX                                                   # followed by text
METADATA_PATTERN += END_METADATA                                                      # followed by ']'
METADATA_PATTERN += ANY_CHARS_REGEX                                                   # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX                                                 # metadata record ends newline
METADATA_MATCHER = re.compile(METADATA_PATTERN)

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
        timestamp_match = TIMESTAMP_MATCHER.match(raw_data)

        dcl_datetime = datetime.strptime(timestamp_match.group(1), "%Y/%m/%d %H:%M:%S.%f")
        # adjust local seconds to get to utc by subtracting timezone seconds
        utc_time = float(dcl_datetime.strftime("%s.%f")) - time.timezone
        self.set_internal_timestamp(unix_time=utc_time)
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        uncorr_match = UNCORR_MATCHER.match(self.raw_data)
        if uncorr_match is not None:
            log.debug('parsing uncorrected particle')
            return [self._encode_value(name, uncorr_match.group(group), function)
                    for name, group, function in UNCORR_PARTICLE_MAP]
        
        else:
            corr_match = CORR_MATCHER.match(self.raw_data)
            if corr_match is not None:
                log.debug('parsing corrected particle')
                return [self._encode_value(name, corr_match.group(group), function)
                    for name, group, function in CORR_PARTICLE_MAP]

            else:
                log.warn("Exception when building parsed values")
                raise RecoverableSampleException("Error WHILE decoding parameters in data: [%s]"
                                                  % self.raw_data)



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




class CtdbpCdefDclCpParser(BufferLoadingParser):
    """
    """
    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        super(CtdbpCdefDclCpParser, self).__init__(config,
                                                stream_handle,
                                                state,
                                                self.sieve_function,
                                                state_callback,
                                                publish_callback,
                                                exception_callback,
                                                *args,
                                                **kwargs)

        self.input_file = stream_handle

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            # Set the particle classes to be used later
            if PARTICLE_CLASS_KEY in particle_classes_dict:
                self._particle_class = particle_classes_dict.get(PARTICLE_CLASS_KEY)
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
 
    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            test_meta = METADATA_MATCHER.match(non_data)
            if test_meta is None:
                self._exception_callback(RecoverableSampleException(
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

        # Initialize the result particles list we will return
        result_particles = []

        # Retrieve the next non data chunk
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
 
        # Retrieve the next data chunk
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
 
        # Process the non data
        self.handle_non_data(non_data, non_end, start)
                      
        # While the data chunk is not None, process the data chunk
        while chunk is not None:


            test_inst = UNCORR_MATCHER.match(chunk)
            test_corr = CORR_MATCHER.match(chunk)

            if test_inst is not None or test_corr is not None:
                # Extract  instrument record particle
                data_particle = self._extract_sample(self._particle_class,
                                                     None,
                                                     chunk,
                                                     None)
                if data_particle is not None:
                    result_particles.append((data_particle, None))
                    
            # Retrieve the next non data chunk
            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)

            # Retrieve the next data chunk
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

            # Process the non data
            self.handle_non_data(non_data, non_end, start)

        return result_particles



    def sieve_function(self, raw_data):
        """
        Sort through the raw data to identify new blocks of data that need processing.
        Monitor state and completeness of multiline wave records, to catch both malformed
        lines as well as those missing.
        """

        data_index = 0
        return_list = []
        raw_data_len = len(raw_data)
        
        
        while data_index < raw_data_len:
            # Do we have a newline delimited field left?
            record_match = CTDBP_RECORD_MATCHER.match(raw_data[data_index:])
            if record_match:
                # check for a metadata line ( a single line )
                test_meta = METADATA_MATCHER.match(record_match.group(0))
                if test_meta is not None:
                    log.debug("meta match")
                    return_list.append((data_index, data_index + len(test_meta.group())))
                    data_index += len(test_meta.group())
                    continue

                # a instrument message, also a single line
                test_inst = UNCORR_MATCHER.match(record_match.group(0))
                if test_inst is not None:
                    log.debug("uncorrected sample match")
                    return_list.append((data_index, data_index + len(test_inst.group())))
                    data_index += len(test_inst.group())
                    continue

                # a corrected instrument message, also a single line
                test_corr = CORR_MATCHER.match(record_match.group(0))
                if test_corr is not None:
                    log.debug("corrected sample match")
                    return_list.append((data_index, data_index + len(test_corr.group())))
                    data_index += len(test_corr.group())
                    continue

               # something in the data didn't match a required regex, so raise an exception and press on.
                data_index += len(record_match.group(0))
                message = "Error while decoding parameters in data: [%s]" % (record_match.group(0))
                self._exception_callback(RecoverableSampleException(message))

            else:
                log.debug("not a complete record left")
                break
       
        log.debug("returning sieve list %s", return_list)
        return return_list

