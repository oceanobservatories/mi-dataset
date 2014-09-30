#!/usr/bin/env python

"""
@package mi.dataset.parser.dcl_file_common
@file marine-integrations/mi/dataset/parser/dcl_file_common.py
@author Ronald Ronquillo
@brief Parser for the file type for the dcl
Release notes:

initial release
"""

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'

import calendar
import re
from functools import partial

from mi.core.log import get_logger
log = get_logger()
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticle
from mi.core.exceptions import UnexpectedDataException

from mi.dataset.dataset_parser import BufferLoadingParser, DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, SPACE_REGEX, \
    ANY_CHARS_REGEX, DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX


# Basic patterns
SPACES = SPACE_REGEX + "+"
START_GROUP = '('
END_GROUP = ')'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has space-delimited fields (date, time, integers)
# All records end with newlines.
TIME = r'(\d{2}):(\d{2}):(\d{2}\.\d{3})'    # Time: HH:MM:SS.mmm
TIMESTAMP = START_GROUP + DATE_YYYY_MM_DD_REGEX + SPACE_REGEX + \
            TIME_HR_MIN_SEC_MSEC_REGEX + END_GROUP
START_METADATA = r'\['
END_METADATA = r'\]'

# All basic dcl records are ASCII characters separated by a newline.
RECORD_PATTERN = ANY_CHARS_REGEX            # Any number of ASCII characters
RECORD_PATTERN += END_OF_LINE_REGEX         # separated by a new line
RECORD_MATCHER = re.compile(RECORD_PATTERN)

SENSOR_GROUP_TIMESTAMP = 0
SENSOR_GROUP_YEAR = 1
SENSOR_GROUP_MONTH = 2
SENSOR_GROUP_DAY = 3
SENSOR_GROUP_HOUR = 4
SENSOR_GROUP_MINUTE = 5
SENSOR_GROUP_SECOND = 6
SENSOR_GROUP_MILLISECOND = 7


class DclInstrumentDataParticle(DataParticle):
    """
    Class for generating the dcl instrument particle.
    """

    def __init__(self, raw_data, instrument_particle_map, *args, **kwargs):

        super(DclInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        timestamp = (
            int(self.raw_data[SENSOR_GROUP_YEAR]),
            int(self.raw_data[SENSOR_GROUP_MONTH]),
            int(self.raw_data[SENSOR_GROUP_DAY]),
            int(self.raw_data[SENSOR_GROUP_HOUR]),
            int(self.raw_data[SENSOR_GROUP_MINUTE]),
            float(self.raw_data[SENSOR_GROUP_SECOND] + "." +
                  self.raw_data[SENSOR_GROUP_MILLISECOND]),
            0, 0, 0)

        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)
        self.instrument_particle_map = instrument_particle_map

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in self.instrument_particle_map]


class DclFileCommonParser(BufferLoadingParser):
    """
    Parser for dcl data.
    In addition to the standard constructor parameters,
    this constructor takes additional parameters sensor_data_matcher
    and metadata_matcher
    """

    def __init__(self,
                 sensor_data_matcher,
                 metadata_matcher,
                 config,
                 stream_handle,
                 *args, **kwargs):

        # Default the position within the file to the beginning.
        self.input_file = stream_handle

        if sensor_data_matcher is not None:
            self.sensor_data_matcher = sensor_data_matcher
        self.metadata_matcher = metadata_matcher

        # No fancy sieve function needed for this parser.
        # File is ASCII with records separated by newlines.
        super(DclFileCommonParser, self).__init__(
            config,
            stream_handle,
            None,
            partial(StringChunker.regex_sieve_function,
                    regex_list=[RECORD_MATCHER]),
            *args, **kwargs)

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
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
        nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
        timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        # Accommodate for any parser not using the PARTICLE_CLASSES_DICT in config
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in self._config:
            particle_classes = self._config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT].values()
        else:
            particle_classes = (self._particle_class,)

        while chunk:

            for particle_class in particle_classes:
                if hasattr(particle_class, "data_matcher"):
                    self.sensor_data_matcher = particle_class.data_matcher

                # If this is a valid sensor data record,
                # use the extracted fields to generate a particle.
                sensor_match = self.sensor_data_matcher.match(chunk)
                if sensor_match is not None:
                    break

            if sensor_match is not None:
                particle = self._extract_sample(particle_class,
                                                None,
                                                sensor_match.groups(),
                                                None)
                if particle is not None:
                    result_particles.append((particle, None))

            # It's not a sensor data record, see if it's a metadata record.
            else:

                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.
                meta_match = self.metadata_matcher.match(chunk)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk %s' % chunk
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))

            nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
            timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles