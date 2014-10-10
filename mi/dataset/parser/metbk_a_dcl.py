#!/usr/bin/env python

"""
@package mi.dataset.parser.metbk_a_dcl
@file marine-integrations/mi/dataset/parser/metbk_a_dcl.py
@author Ronald Ronquillo
@brief Parser for the metbk_a_dcl dataset driver

This file contains code for the metbk_a_dcl parsers and code to produce data particles.
For telemetered data, there is one parser which produces one type of data particle.
For recovered data, there is one parser which produces one type of data particle.
The input files and the content of the data particles are the same for both
recovered and telemetered.
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

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    DclFileCommonParser, SENSOR_GROUP_TIMESTAMP, ANY_CHARS, SPACE, TIMESTAMP,\
    START_METADATA, END_METADATA

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, FLOAT_REGEX
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


MODULE_NAME = 'mi.dataset.parser.metbk_a_dcl'
RECOVERED_PARTICLE_CLASS = 'MetbkADclRecoveredInstrumentDataParticle'
TELEMETERED_PARTICLE_CLASS = 'MetbkADclTelemeteredInstrumentDataParticle'

# Basic patterns
CFLOAT_REGEX = '(' + FLOAT_REGEX + ')'  # floating point as a captured group

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE    # dcl controller timestamp
METADATA_PATTERN += START_METADATA      # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS           # followed by text
METADATA_PATTERN += END_METADATA        # followed by ']'
METADATA_PATTERN += ANY_CHARS           # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX   # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)


# Sensor data record:
#   Timestamp Date<tab>Time<tab>SensorData
#   where SensorData are space-separated floating point numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACE         # dcl controller timestamp
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Barometric Pressure
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Relative Humidity
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Air Temperature
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Longwave Irradiance
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Precipitation Level
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Sea Surface Temperature
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Sea Surface Conductivity
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Shortwave Irradiance
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Eastward Wind Velocity relative to Magnetic North
SENSOR_DATA_PATTERN += CFLOAT_REGEX + SPACE     # Northward Wind Velocity relative to Magnetic North
SENSOR_DATA_PATTERN += FLOAT_REGEX + SPACE      # Not Applicable (Don't Care)
SENSOR_DATA_PATTERN += FLOAT_REGEX              # Not Applicable (Don't Care)
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX        # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)


# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER
# incremented after common timestamp values.
# i.e, match.groups()[INDEX]
SENSOR_GROUP_BAROMETRIC_PRESSURE = 7
SENSOR_GROUP_RELATIVE_HUMIDITY = 8
SENSOR_GROUP_AIR_TEMPERATURE = 9
SENSOR_GROUP_LONGWAVE_IRRADIANCE = 10
SENSOR_GROUP_PRECIPITATION = 11
SENSOR_GROUP_SEA_SURFACE_TEMPERATURE = 12
SENSOR_GROUP_SEA_SURFACE_CONDUCTIVITY = 13
SENSOR_GROUP_SHORTWAVE_IRRADIANCE = 14
SENSOR_GROUP_EASTWARD_WIND_VELOCITY = 15
SENSOR_GROUP_NORTHWARD_WIND_VELOCITY = 16

# This table is used in the generation of the instrument data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
INSTRUMENT_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    SENSOR_GROUP_TIMESTAMP,                 str),
    ('barometric_pressure',         SENSOR_GROUP_BAROMETRIC_PRESSURE,       float),
    ('relative_humidity',           SENSOR_GROUP_RELATIVE_HUMIDITY,         float),
    ('air_temperature',             SENSOR_GROUP_AIR_TEMPERATURE,           float),
    ('longwave_irradiance',         SENSOR_GROUP_LONGWAVE_IRRADIANCE,       float),
    ('precipitation',               SENSOR_GROUP_PRECIPITATION,             float),
    ('sea_surface_temperature',     SENSOR_GROUP_SEA_SURFACE_TEMPERATURE,   float),
    ('sea_surface_conductivity',    SENSOR_GROUP_SEA_SURFACE_CONDUCTIVITY,  float),
    ('shortwave_irradiance',        SENSOR_GROUP_SHORTWAVE_IRRADIANCE,      float),
    ('eastward_wind_velocity',      SENSOR_GROUP_EASTWARD_WIND_VELOCITY,    float),
    ('northward_wind_velocity',     SENSOR_GROUP_NORTHWARD_WIND_VELOCITY,   float)
]


def process(source_file_path, particle_data_hdlr_obj, particle_class):

    with open(source_file_path, "r") as stream_handle:
        parser = MetbkADclParser(
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
    REC_INSTRUMENT_PARTICLE = 'metbk_a_dcl_instrument_recovered'
    TEL_INSTRUMENT_PARTICLE = 'metbk_a_dcl_instrument'


class MetbkADclInstrumentDataParticle(DclInstrumentDataParticle):
    """
    Class for generating the Metbk_a instrument particle.
    """
    def __init__(self, raw_data, *args, **kwargs):

        super(MetbkADclInstrumentDataParticle, self).__init__(
            raw_data,
            INSTRUMENT_PARTICLE_MAP,
            *args, **kwargs)


class MetbkADclRecoveredInstrumentDataParticle(MetbkADclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class MetbkADclTelemeteredInstrumentDataParticle(MetbkADclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_INSTRUMENT_PARTICLE


class MetbkADclParser(DclFileCommonParser):
    """
    This is the entry point for the Metbk_a_dcl parser.
    """
    def __init__(self, *args, **kwargs):

        super(MetbkADclParser, self).__init__(SENSOR_DATA_MATCHER,
                                              METADATA_MATCHER,
                                              *args, **kwargs)