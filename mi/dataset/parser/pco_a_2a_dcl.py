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

import re

from mi.core.log import get_logger

from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    DclFileCommonParser, SENSOR_GROUP_TIMESTAMP, ANY_CHARS, SPACE, TIMESTAMP,\
    START_METADATA, END_METADATA, START_GROUP, END_GROUP

from mi.core.exceptions import UnexpectedDataException

MODULE_NAME = 'mi.dataset.parser.pco_a_2a_dcl'
RECOVERED_AIR_PARTICLE_CLASS = 'Pco2aDclAirRecoveredInstrumentDataParticle'
TELEMETERED_AIR_PARTICLE_CLASS = 'Pco2aDclAirInstrumentDataParticle'
RECOVERED_WATER_PARTICLE_CLASS = 'Pco2aDclWaterRecoveredInstrumentDataParticle'
TELEMETERED_WATER_PARTICLE_CLASS = 'Pco2aDclWaterInstrumentDataParticle'

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, UNSIGNED_INT_REGEX
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
log = get_logger()

# Basic patterns
UINT = '('+UNSIGNED_INT_REGEX+')'   # unsigned integer as a group
FLOAT = '('+FLOAT_REGEX+')'         # floating point as a captured group
W_A_CHAR = r'[W|A]'
ANY_CHAR = r'(\D)'
SPACE = ' '
COMMA = ','
SHARP = '#'
CHAR_M = ' *M'


# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
SENSOR_DATE = r'(\d{4}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2})'  # Sensor Time: HH:MM:SS

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ANY_CHARS  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Sensor data record:
#   Timestamp Date<space>Time<space>SensorData
#   where SensorData are comma-separated unsigned integer numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACE  # dcl controller timestamp
SENSOR_DATA_PATTERN += SHARP + START_GROUP + SENSOR_DATE + SPACE  # sensor date
SENSOR_DATA_PATTERN += SENSOR_TIME + END_GROUP + COMMA + CHAR_M + COMMA  # sensor time
SENSOR_DATA_PATTERN += UINT + COMMA         # measurement wavelength beta
SENSOR_DATA_PATTERN += UINT + COMMA         # raw signal beta
SENSOR_DATA_PATTERN += FLOAT + COMMA        # measurement wavelength chl
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal chl
SENSOR_DATA_PATTERN += FLOAT + COMMA        # measurement wavelength cdom
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal cdom
SENSOR_DATA_PATTERN += UINT + COMMA         # raw signal beta
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal cdom
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal cdom
SENSOR_DATA_PATTERN += ANY_CHAR             # raw internal temperature
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX    # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER.
# i.e, match.groups()[INDEX]
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


def process(source_file_path, particle_data_hdlr_obj, particle_class):

    with open(source_file_path, "r") as stream_handle:
        parser = Pco2aDclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            stream_handle,
            lambda state, ingested: None,
            lambda data: log.trace("Found data: %s", data),
            lambda ex: particle_data_hdlr_obj.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_hdlr_obj)
        driver.processFileStream()


class DataParticleType(BaseEnum):
    PCO2A_INSTRUMENT_AIR_PARTICLE = 'pco2a_a_dcl_instrument_air'
    PCO2A_INSTRUMENT_AIR_RECOVERED_PARTICLE = 'pco2a_a_dcl_instrument_air_recovered'
    PCO2A_INSTRUMENT_WATER_PARTICLE = 'pco2a_a_dcl_instrument_water'
    PCO2A_INSTRUMENT_WATER_RECOVERED_PARTICLE = 'pco2a_a_dcl_instrument_water_recovered'
    PCO2A_META_PARTICLE = 'pco2a_a_dcl_metadata'
    PCO2A_META_RECOVERED_PARTICLE = 'pco2a_a_dcl_metadata_recovered'


class Pco2aDclInstrumentDataParticle(DclInstrumentDataParticle):
    """
    Class for generating the pco 2a dcl instrument particle.
    """
    def __init__(self, raw_data, *args, **kwargs):

        super(Pco2aDclInstrumentDataParticle, self).__init__(
            raw_data,
            INSTRUMENT_PARTICLE_MAP,
            *args, **kwargs)


class Pco2aDclInstrumentDataParticle_water(DclInstrumentDataParticle):
    """
    Class for generating the pco 2a dcl instrument particle.
    """
    def __init__(self, raw_data, *args, **kwargs):

        super(Pco2aDclInstrumentDataParticle_water, self).__init__(
            raw_data,
            INSTRUMENT_PARTICLE_WATER_MAP,
            *args, **kwargs)


class Pco2aDclAirInstrumentDataParticle(Pco2aDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered air data.
    """
    _test_variable = None
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


class Pco2aDclParser(DclFileCommonParser):
    """
    This is the entry point for the Metbk_a_dcl parser.
    """
    def __init__(self, *args, **kwargs):

        super(Pco2aDclParser, self).__init__(SENSOR_DATA_MATCHER,
                                             METADATA_MATCHER,
                                             *args, **kwargs)

#override comment
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

        while chunk is not None:

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

            sensor_match = self.sensor_data_matcher.match(chunk)
            if sensor_match is not None:


                if sensor_match.group(sensor_match.lastindex) is 'W':
                    if self._particle_class is Pco2aDclAirInstrumentDataParticle:
                        self._particle_class = Pco2aDclWaterInstrumentDataParticle
                    if self._particle_class is Pco2aDclAirRecoveredInstrumentDataParticle:
                        self._particle_class = Pco2aDclWaterRecoveredInstrumentDataParticle
                    if self._particle_class is Pco2aDclWaterInstrumentDataParticle:
                        pass
                    if self._particle_class is Pco2aDclWaterRecoveredInstrumentDataParticle:
                        pass

                if sensor_match.group(sensor_match.lastindex) is 'A':
                    if self._particle_class is Pco2aDclAirInstrumentDataParticle:
                        pass
                    if self._particle_class is Pco2aDclAirRecoveredInstrumentDataParticle:
                        pass
                    if self._particle_class is Pco2aDclWaterInstrumentDataParticle:
                        self._particle_class = Pco2aDclAirInstrumentDataParticle
                    if self._particle_class is Pco2aDclWaterRecoveredInstrumentDataParticle:
                        self._particle_class = Pco2aDclAirRecoveredInstrumentDataParticle


                particle = self._extract_sample(self._particle_class,
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