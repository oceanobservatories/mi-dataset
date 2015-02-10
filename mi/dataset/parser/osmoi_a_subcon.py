#!/usr/bin/env python

"""
@package mi.dataset.parser.osmoi_a_subcon
@file marine-integrations/mi/dataset/parser/osmoi_a_subcon.py
@author Rachel Manoni
@brief Parser for the osmoi_a_subcon dataset driver

Release notes:

Initial Release
"""

__author__ = 'Rachel Manoni'
__license__ = 'Apache 2.0'

import calendar
import time
import csv
import math

from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.dataset.dataset_parser import SimpleParser
from mi.core.log import get_logger
log = get_logger()

OSMOI_DATA_MAP = [
    ('serial_number', 1, str),
    ('start_time', 2, str),
    ('stop_time', 3, str),
    ('port_number', 4, int),
    ('volume_pumped', 5, float),
    ('lat', 6, float),
    ('lon', 7, float),
    ('depth', 8, int),
    ('temp', 9, float),
    ('acidity', 10, float),
    ('alkalinity', 11, float),
    ('hydrogen_sulfide', 12, float),
    ('silicon', 13, float),
    ('ammonia', 14, float),
    ('chloride', 15, float),
    ('sulfate', 16, float),
    ('sodium', 17, float),
    ('potassium', 18, float),
    ('magnesium', 19, float),
    ('calcium', 20, float),
    ('bromide', 21, float),
    ('iron', 22, float),
    ('manganese', 23, float),
    ('lithium', 24, float),
    ('strontium_icpaes', 25, float),
    ('boron', 26, float),
    ('rubidium', 27, float),
    ('cesium', 28, float),
    ('strontium_icpms', 29, float),
    ('barium', 30, float),
    ('cobalt', 31, float),
    ('nickel', 32, float),
    ('copper', 33, float),
    ('zinc', 34, float),
    ('molybdenum', 35, float),
    ('silver', 36, float),
    ('cadmium', 37, float),
    ('titanium', 38, float),
    ('aluminum', 39, float),
    ('lead', 40, float),
    ('vanadium', 41, float),
    ('uranium', 42, float),
    ('yttrium', 43, float),
    ('gadolinium', 44, float)
]

START_TIME_IDX = 2


class OsmoiASubconInstrumentDataParticle(DataParticle):

    _data_particle_type = 'osmoi_a_subcon_instrument_recovered'
    instrument_particle_map = OSMOI_DATA_MAP

    def __init__(self, raw_data, *args, **kwargs):

        super(OsmoiASubconInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # set timestamp mm/dd/yyyy hh:mm
        struct_time = time.strptime(self.raw_data[START_TIME_IDX], "%m/%d/%Y %H:%M")
        elapsed_seconds = calendar.timegm(struct_time)
        self.set_internal_timestamp(unix_time=elapsed_seconds)

    def _build_parsed_values(self):
        """
        Build parsed values for the Instrument Data Particle.
        """
        return [self._encode_value(name, self.raw_data[group], function)
                for name, group, function in self.instrument_particle_map]

    def _encode_value(self, name, value, encoding_function):
        """
        Encode a value using the encoding function, if it fails store the error in a queue
        """
        try:
            if encoding_function is float and math.isnan(float(value)):
                encoded_val = None
            else:
                encoded_val = encoding_function(value)
        except Exception as e:
            log.warn("Data particle error encoding. Name:%s Value:%s", name, value)
            encoded_val = None
        finally:
            return {DataParticleKey.VALUE_ID: name,
                    DataParticleKey.VALUE: encoded_val}


class OsmoiASubconParser(SimpleParser):

    def parse_file(self):
        """
        Parse a .csv input file. Build a data particle from the parsed data.
        """
        reader = csv.reader(self._stream_handle)
        for row in reader:
            log.debug("Read in line: %r", row)

            try:
                # Extract a particle and append it to the record buffer
                particle = self._extract_sample(OsmoiASubconInstrumentDataParticle, None, row, None)
                self._record_buffer.append(particle)
            except:
                log.warn("Data cannot be parsed: %r", row)