#!/usr/bin/env python

"""
@package mi.dataset.parser.pco_a_2a_dcl
@file marine-integrations/mi/dataset/parser/pco_a_2a_dcl.py
@author Sung Ahn
@brief Parser for the pco_a_2a_dcl dataset driver

This file contains code for the pco_2a_dj_dcl parsers and code to produce data particles.
For instrument data, there is one parser which produces two (air/water) type of data particle.
For instrument recover data, there is one parser which produces two(air/water) type of data particle.
The input files and the content of the data particles are the same for both
instrument and instrument recovered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 2 types of records.
Records are separated by a newline.
All records start with a timestamp.
Metadata records: timestamp [text] more text newline.
Sensor Data records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release
"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import calendar
import copy
from functools import partial
import re

from mi.core.instrument.chunker import \
    StringChunker

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.exceptions import \
    DatasetParserException, \
    UnexpectedDataException

from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.dataset.dataset_parser import BufferLoadingParser

log = get_logger()

# Basic patterns
ANY_CHARS = r'.*'  # Any characters excluding a newline
NEW_LINE = r'(?:\r\n|\n)'  # any type of new line
UINT = r'(\d*)'  # unsigned integer as a group
FLOAT = r'(\d+.\d+)'
W_A_CHAR = r'[W|A]'
ANY_CHAR = r'(\D)'
SPACE = ' '
TAB = '\t'
COMMA = ','
SHARP = '#'
CHAR_M = ' *M'

START_GROUP = '('
END_GROUP = ')'


# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
DATE = r'(\d{4})/(\d{2})/(\d{2})'  # Date: YYYY/MM/DD
TIME = r'(\d{2}):(\d{2}):(\d{2}\.\d{3})'  # Time: HH:MM:SS.mmm
SENSOR_DATE = r'(\d{4}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2})'  # Sensor Time: HH:MM:SS
TIMESTAMP = START_GROUP + DATE + SPACE + TIME + END_GROUP
START_METADATA = r'\['
END_METADATA = r'\]'
END_LF = r'\n'

# All pco records are ASCII characters separated by a newline.
PCO_RECORD_PATTERN = ANY_CHARS  # Any number of ASCII characters
PCO_RECORD_PATTERN += NEW_LINE  # separated by a new line
PCO_RECORD_MATCHER = re.compile(PCO_RECORD_PATTERN)

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ANY_CHARS  # followed by more text
METADATA_PATTERN += END_LF  # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Sensor data record:
#   Timestamp Date<space>Time<space>SensorData
#   where SensorData are comma-separated unsigned integer numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACE  # dcl controller timestamp
SENSOR_DATA_PATTERN += SHARP + START_GROUP + SENSOR_DATE + SPACE  # sensor date
SENSOR_DATA_PATTERN += SENSOR_TIME + END_GROUP + COMMA + CHAR_M + COMMA  # sensor time
SENSOR_DATA_PATTERN += UINT + COMMA  # measurement wavelength beta
SENSOR_DATA_PATTERN += UINT + COMMA  # raw signal beta
SENSOR_DATA_PATTERN += FLOAT + COMMA  # measurement wavelength chl
SENSOR_DATA_PATTERN += FLOAT + COMMA  # raw signal chl
SENSOR_DATA_PATTERN += FLOAT + COMMA  # measurement wavelength cdom
SENSOR_DATA_PATTERN += FLOAT + COMMA  # raw signal cdom
SENSOR_DATA_PATTERN += UINT + COMMA  # raw signal beta
SENSOR_DATA_PATTERN += FLOAT + COMMA  # raw signal cdom
SENSOR_DATA_PATTERN += FLOAT + COMMA  # raw signal cdom
SENSOR_DATA_PATTERN += ANY_CHAR  # raw internal temperature
SENSOR_DATA_PATTERN += END_LF  # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER.
# i.e, match.groups()[INDEX]
SENSOR_GROUP_TIMESTAMP = 0
SENSOR_GROUP_YEAR = 1
SENSOR_GROUP_MONTH = 2
SENSOR_GROUP_DAY = 3
SENSOR_GROUP_HOUR = 4
SENSOR_GROUP_MINUTE = 5
SENSOR_GROUP_SECOND = 6

SENSOR_GROUP_SENSOR_DATE_TIME = 7
SENSOR_GROUP_SENSOR_DATE = 8
SENSOR_GROUP_SENSOR_TIME = 9
SENSOR_GROUP_ZERO_A2D = 10
SENSOR_GROUP_CURRENT_A2D = 11
SENSOR_GROUP_CO2 = 12
SENSOR_GROUP_AVG_IRGA_TEMP = 13
SENSOR_GROUP_HUMIDITY = 14
SENSOR_GROUP_HUMIDITY_TEMP = 15
SENSOR_GROUP_STREAM_PRESSURE = 16
SENSOR_GROUP_DETECTOR_TEMP = 17
SENSOR_GROUP_SOURCE_TEMP = 18

INSTRUMENT_PARTICLE_MAP = [
    ('dcl_controller_timestamp', SENSOR_GROUP_TIMESTAMP, str),
    ('date_time_string', SENSOR_GROUP_SENSOR_DATE_TIME, str),
    ('zero_a2d', SENSOR_GROUP_ZERO_A2D, int),
    ('current_a2d', SENSOR_GROUP_CURRENT_A2D, int),
    ('measured_air_co2', SENSOR_GROUP_CO2, float),
    ('avg_irga_temperature', SENSOR_GROUP_AVG_IRGA_TEMP, float),
    ('humidity', SENSOR_GROUP_HUMIDITY, float),
    ('humidity_temperature', SENSOR_GROUP_HUMIDITY_TEMP, float),
    ('gas_stream_pressure', SENSOR_GROUP_STREAM_PRESSURE, int),
    ('irga_detector_temperature', SENSOR_GROUP_DETECTOR_TEMP, float),
    ('irga_source_temperature', SENSOR_GROUP_SOURCE_TEMP, float)
]

INSTRUMENT_PARTICLE_WATER_MAP = [
    ('dcl_controller_timestamp', SENSOR_GROUP_TIMESTAMP, str),
    ('date_time_string', SENSOR_GROUP_SENSOR_DATE_TIME, str),
    ('zero_a2d', SENSOR_GROUP_ZERO_A2D, int),
    ('current_a2d', SENSOR_GROUP_CURRENT_A2D, int),
    ('measured_water_co2', SENSOR_GROUP_CO2, float),
    ('avg_irga_temperature', SENSOR_GROUP_AVG_IRGA_TEMP, float),
    ('humidity', SENSOR_GROUP_HUMIDITY, float),
    ('humidity_temperature', SENSOR_GROUP_HUMIDITY_TEMP, float),
    ('gas_stream_pressure', SENSOR_GROUP_STREAM_PRESSURE, int),
    ('irga_detector_temperature', SENSOR_GROUP_DETECTOR_TEMP, float),
    ('irga_source_temperature', SENSOR_GROUP_SOURCE_TEMP, float)
]


class PCOStateKey(BaseEnum):
    POSITION = 'position'  # position within the input file


class DataParticleType(BaseEnum):
    PCO2A_INSTRUMENT_AIR_PARTICLE = 'pco2a_a_dcl_instrument_air'
    PCO2A_INSTRUMENT_AIR_RECOVERED_PARTICLE = 'pco2a_a_dcl_instrument_air_recovered'
    PCO2A_INSTRUMENT_WATER_PARTICLE = 'pco2a_a_dcl_instrument_water'
    PCO2A_INSTRUMENT_WATER_RECOVERED_PARTICLE = 'pco2a_a_dcl_instrument_water_recovered'
    PCO2A_META_PARTICLE = 'pco2a_a_dcl_metadata'
    PCO2A_META_RECOVERED_PARTICLE = 'pco2a_a_dcl_metadata_recovered'


class Pco2aDclInstrumentDataParticle(DataParticle):
    """
    Class for generating the pco 2a dcl instrument particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(Pco2aDclInstrumentDataParticle, self).__init__(raw_data,
                                                             port_timestamp,
                                                             internal_timestamp,
                                                             preferred_timestamp,
                                                             quality_flag,
                                                             new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.

        timestamp = (
            int(self.raw_data[SENSOR_GROUP_YEAR]),
            int(self.raw_data[SENSOR_GROUP_MONTH]),
            int(self.raw_data[SENSOR_GROUP_DAY]),
            int(self.raw_data[SENSOR_GROUP_HOUR]),
            int(self.raw_data[SENSOR_GROUP_MINUTE]),
            float(self.raw_data[SENSOR_GROUP_SECOND]),
            0, 0, 0)
        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)

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
                for name, group, function in INSTRUMENT_PARTICLE_MAP]


class Pco2aDclInstrumentDataParticle_water(DataParticle):
    """
    Class for generating the pco 2a dcl instrument particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(Pco2aDclInstrumentDataParticle_water, self).__init__(raw_data,
                                                                   port_timestamp,
                                                                   internal_timestamp,
                                                                   preferred_timestamp,
                                                                   quality_flag,
                                                                   new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.

        timestamp = (
            int(self.raw_data[SENSOR_GROUP_YEAR]),
            int(self.raw_data[SENSOR_GROUP_MONTH]),
            int(self.raw_data[SENSOR_GROUP_DAY]),
            int(self.raw_data[SENSOR_GROUP_HOUR]),
            int(self.raw_data[SENSOR_GROUP_MINUTE]),
            float(self.raw_data[SENSOR_GROUP_SECOND]),
            0, 0, 0)
        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)

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
                for name, group, function in INSTRUMENT_PARTICLE_WATER_MAP]


class Pco2aDclAirInstrumentDataParticle(Pco2aDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered air data.
    """
    _data_particle_type = DataParticleType.PCO2A_INSTRUMENT_AIR_PARTICLE


class Pco2aDclAirRecoveredInstrumentDataParticle(Pco2aDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered air data.
    """
    _data_particle_type = DataParticleType.PCO2A_INSTRUMENT_AIR_RECOVERED_PARTICLE


class Pco2aDclWaterInstrumentDataParticle(Pco2aDclInstrumentDataParticle_water):
    """
    Class for generating Offset Data Particles from Telemetered  water data.
    """
    _data_particle_type = DataParticleType.PCO2A_INSTRUMENT_WATER_PARTICLE


class Pco2aDclWaterRecoveredInstrumentDataParticle(Pco2aDclInstrumentDataParticle_water):
    """
    Class for generating Offset Data Particles from Recovered water data.
    """
    _data_particle_type = DataParticleType.PCO2A_INSTRUMENT_WATER_RECOVERED_PARTICLE


class Pco2aDclParser(BufferLoadingParser):
    """
    Parser for pco_2a_dcl data.
    In addition to the standard constructor parameters,
    this constructor takes an additional parameter particle_class.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 particle_class,
                 *args, **kwargs):

        # No fancy sieve function needed for this parser.
        # File is ASCII with records separated by newlines.

        super(Pco2aDclParser, self).__init__(config,
                                             stream_handle,
                                             state,
                                             partial(StringChunker.regex_sieve_function,
                                                     regex_list=[PCO_RECORD_MATCHER]),
                                             state_callback,
                                             publish_callback,
                                             exception_callback,
                                             *args,
                                             **kwargs)

        # Default the position within the file to the beginning.
        self._read_state = {PCOStateKey.POSITION: 0}
        self.input_file = stream_handle
        self.particle_class = particle_class

        # If there's an existing state, update to it.

        if state is not None:
            self.set_state(state)

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            self._increment_position(len(non_data))
            self._exception_callback(UnexpectedDataException(
                "Found %d bytes of un-expected non-data %s" %
                (len(non_data), non_data)))

    def _increment_position(self, bytes_read):
        """
        Increment the position within the file.
        @param bytes_read The number of bytes just read
        """
        self._read_state[PCOStateKey.POSITION] += bytes_read

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
            self._increment_position(len(chunk))

            # If this is a valid sensor data record,
            # use the extracted fields to generate a particle.

            # Manual test is below
            # >>me = re.match(r"((\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})) #((\d{4}/\d{2}/\d{2})
            #                (\d{2}:\d{2}:\d{2})), *M,(\d*),(\d*),(\d+.\d+),(\d+.\d+),(\d+.\d+),(\d+.\d+),(\d*),
            #                (\d+.\d+),(\d+.\d+),(\D)",
            #                "2014/08/10 00:20:24.274 #3765/07/27 01:00:11, M,43032,40423,397.04,40.1,21.221,
            #                 28.480,1026,39.9,40.4,W")
            # >>> me.group()
            # '2014/08/10 00:20:24.274 #3765/07/27 01:00:11, M,43032,40423,397.04,40.1,21.221,28.480,1026,39.9,40.4,W'
            # >>me.group(me.lastindex)
            # 'W'

            sensor_match = SENSOR_DATA_MATCHER.match(chunk)
            particle = None
            if sensor_match is not None:

                if sensor_match.group(sensor_match.lastindex) is 'W':
                    if self.particle_class is Pco2aDclAirInstrumentDataParticle:
                        self.particle_class = Pco2aDclWaterInstrumentDataParticle
                    if self.particle_class is Pco2aDclAirRecoveredInstrumentDataParticle:
                        self.particle_class = Pco2aDclWaterRecoveredInstrumentDataParticle
                    if self.particle_class is Pco2aDclWaterInstrumentDataParticle:
                        pass
                    if self.particle_class is Pco2aDclWaterRecoveredInstrumentDataParticle:
                        pass

                    particle = self._extract_sample(self.particle_class,
                                                    None,
                                                    sensor_match.groups(),
                                                    None)

                if sensor_match.group(sensor_match.lastindex) is 'A':
                    if self.particle_class is Pco2aDclAirInstrumentDataParticle:
                        pass
                    if self.particle_class is Pco2aDclAirRecoveredInstrumentDataParticle:
                        pass
                    if self.particle_class is Pco2aDclWaterInstrumentDataParticle:
                        self.particle_class = Pco2aDclAirInstrumentDataParticle
                    if self.particle_class is Pco2aDclWaterRecoveredInstrumentDataParticle:
                        self.particle_class = Pco2aDclAirRecoveredInstrumentDataParticle

                    particle = self._extract_sample(self.particle_class,
                                                    None,
                                                    sensor_match.groups(),
                                                    None)

                if particle is not None:
                    result_particles.append((particle, copy.copy(self._read_state)))

            # It's not a sensor data record, see if it's a metadata record.

            else:

                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.

                meta_match = METADATA_MATCHER.match(chunk)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk %s' % chunk
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to.
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")

        if not (PCOStateKey.POSITION in state_obj):
            raise DatasetParserException('%s missing in state keys' %
                                         PCOStateKey.POSITION)

        self._record_buffer = []
        self._state = state_obj
        self._read_state = state_obj

        self.input_file.seek(state_obj[PCOStateKey.POSITION])


class Pco2aDclAirParser(Pco2aDclParser):
    """
    This is the entry point for the Telemetered pco_2a_dcl parser.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):
        super(Pco2aDclAirParser, self).__init__(config,
                                                stream_handle,
                                                state,
                                                state_callback,
                                                publish_callback,
                                                exception_callback,
                                                Pco2aDclAirInstrumentDataParticle,
                                                *args,
                                                **kwargs)


class Pco2aDclAirRecoveredParser(Pco2aDclParser):
    """
    This is the entry point for the Recovered pco_2a_dcl parser.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):
        super(Pco2aDclAirRecoveredParser, self).__init__(config,
                                                         stream_handle,
                                                         state,
                                                         state_callback,
                                                         publish_callback,
                                                         exception_callback,
                                                         Pco2aDclAirRecoveredInstrumentDataParticle,
                                                         *args,
                                                         **kwargs)


class Pco2aDclWaterParser(Pco2aDclParser):
    """
    This is the entry point for the Telemetered pco_2a_dcl parser.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):
        super(Pco2aDclWaterParser, self).__init__(config,
                                                  stream_handle,
                                                  state,
                                                  state_callback,
                                                  publish_callback,
                                                  exception_callback,
                                                  Pco2aDclWaterInstrumentDataParticle,
                                                  *args,
                                                  **kwargs)


class Pco2aDclWaterRecoveredParser(Pco2aDclParser):
    """
    This is the entry point for the Recovered pco_2a_dcl parser.
    """

    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):
        super(Pco2aDclWaterRecoveredParser, self).__init__(config,
                                                           stream_handle,
                                                           state,
                                                           state_callback,
                                                           publish_callback,
                                                           exception_callback,
                                                           Pco2aDclWaterRecoveredInstrumentDataParticle,
                                                           *args,
                                                           **kwargs)
