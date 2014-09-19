__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

import ntplib
import re
import time
import gc

from mi.core.log import get_logger

log = get_logger()
from mi.dataset.dataset_parser import Parser
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDataParticleKey, Pco2wAbcInstrumentDataParticle, \
    Pco2wAbcInstrumentBlankDataParticle, Pco2wAbcMetadataDataParticle, Pco2wAbcPowerDataParticle
from mi.dataset.parser.common_regexes import UNSIGNED_INT_REGEX, ONE_OR_MORE_WHITESPACE_REGEX

"""
*** BEGIN definition of regular expressions, matchers and group indices for instrument and instrument blank
data records.

Example instrument record with light measurements:
> 4	3466998237	44	38	2869	311	1877	613	1581	5139	42	38	2887	313	1879	614	3200	2248

Data Mapping:
4 -> record type (instrument data)
3466998237 -> record time in seconds since 1904 -> ?
44	38	2869	311	1877	613	1581	5139	42	38	2887	313	1879	614 -> light measurements
3200 -> battery voltage
2248 -> thermistor

Example instrument record with blank light measurements:
> 5	3466968983	41	36	2919	1566	1921	1763	8707	15056	42	38	2933	1575	1925	1766
3180	2138

Data Mapping:
5 -> record type (instrument blank data)
3466968983 -> record time seconds since 1904 -> 2013-11-10 22:56:23
41 36 2919 1566 1921 1763 8707 15056 42 38 2933 1575 1925 1766 -> blank light measurements
3180 -> battery voltage
2138 -> thermistor
"""
INSTRUMENT_RECORD_TYPE_REGEX = r'(4)'
INSTRUMENT_BLANK_RECORD_TYPE_REGEX = r'(5)'
RECORD_TIME_REGEX = BATTERY_VOLTAGE_REGEX = THERMISTOR_REGEX = r'(' + UNSIGNED_INT_REGEX + ')'
LIGHT_MEASUREMENTS_REGEX = r'((?:' + UNSIGNED_INT_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + '){14})'

INSTRUMENT_REGEX = r'^' + INSTRUMENT_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                   RECORD_TIME_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + LIGHT_MEASUREMENTS_REGEX + \
                   BATTERY_VOLTAGE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + THERMISTOR_REGEX
INSTRUMENT_MATCHER = re.compile(INSTRUMENT_REGEX)

INSTRUMENT_BLANK_REGEX = r'^' + INSTRUMENT_BLANK_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                         RECORD_TIME_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + LIGHT_MEASUREMENTS_REGEX + \
                         BATTERY_VOLTAGE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + THERMISTOR_REGEX
INSTRUMENT_BLANK_MATCHER = re.compile(INSTRUMENT_BLANK_REGEX)

RECORD_TYPE_GROUP_INDEX = 1
RECORD_TIME_GROUP_INDEX = 2
LIGHT_MEASUREMENTS_GROUP_INDEX = 3
BATTERY_VOLTAGE_GROUP_INDEX = 4
THERMISTOR_GROUP_INDEX = 5
"""
*** END definition of regular expressions, matchers and group indices for instrument and instrument blank data records.
"""

"""
*** BEGIN definition of regular expressions, matchers and group indices for metadata records.

Metadata record types:
128 - launch record type
129 - start record type
131 - good shutdown record type
133 - handshake_on record type
135 - stopped by user record type

Example:
> 128	3466968623	65	1	0	512

Data Mapping:
128 -> launch record type
3466968623 -> seconds since 1904 -> 2013-11-10 22:50:23
65 -> flag = 0x41 = 01000001 -> erased, clock started
1 -> number of records
0 -> number of errors
512 ->number of bytes
"""
FLAGS_REGEX = r'(\d{2})'
NUM_DATA_RECORDS_REGEX = NUM_ERROR_RECORDS_REGEX = NUM_BYTES_STORED_REGEX = \
    r'(' + UNSIGNED_INT_REGEX + ')'
METADATA_RECORD_TYPE_REGEX = r'(128|129|131|133|135)'
METADATA_REGEX = r'^' + METADATA_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                 RECORD_TIME_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + FLAGS_REGEX + \
                 ONE_OR_MORE_WHITESPACE_REGEX + METADATA_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                 NUM_ERROR_RECORDS_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + NUM_BYTES_STORED_REGEX
METADATA_MATCHER = re.compile(METADATA_REGEX)

FLAGS_GROUP_INDEX = 3
NUM_DATA_RECORDS_GROUP_INDEX = 4
NUM_ERROR_RECORDS_GROUP_INDEX = 5
NUM_BYTES_STORED_GROUP_INDEX = 6

CLOCK_ACTIVE_FLAGS_INDEX = 0
RECORDING_ACTIVE_FLAGS_INDEX = 1
RECORD_END_ON_TIME_FLAGS_INDEX = 2
RECORD_MEMORY_FULL_FLAGS_INDEX = 3
RECORD_END_ON_ERROR_FLAGS_INDEX = 4
DATA_DOWNLOAD_OK_FLAGS_INDEX = 5
FLASH_MEMORY_OPEN_FLAGS_INDEX = 6
BATTERY_LOW_PRESTART_FLAGS_INDEX = 7
BATTERY_LOW_MEASUREMENT_FLAGS_INDEX = 8
BATTERY_LOW_BLANK_FLAGS_INDEX = 9
BATTERY_LOW_EXTERNAL_FLAGS_INDEX = 10
EXTERNAL_DEVICE1_FAULT_FLAGS_INDEX = 11
EXTERNAL_DEVICE2_FAULT_FLAGS_INDEX = 12
EXTERNAL_DEVICE3_FAULT_FLAGS_INDEX = 13
FLASH_ERASED_FLAGS_INDEX = 14
POWER_ON_INVALID_FLAGS_INDEX = 15
"""
*** END definition of regular expressions, matchers and group indices for metadata records.
"""

"""
*** BEGIN definition of regular expressions, matchers and group indices for power records.

Power record type:

Example:
> 17	3466968635

Data Mapping:
17 -> power record type
3466968635 -> seconds since 1904 Sunday, November 10, 2013 5:50:35pm
"""

POWER_RECORD_TYPE_REGEX = r'(17)'
POWER_REGEX = r'^' + POWER_RECORD_TYPE_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
              RECORD_TIME_REGEX
POWER_MATCHER = re.compile(POWER_REGEX)
"""
*** END definition of regular expressions, matchers and group indices for power records.
"""

"""
NOTE: A record with record type 10 should be ignored.
"""


class Pco2wAbcParser(Parser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 state_callback=None,  # No longer used
                 publish_callback=None,  # No longer used
                 *args, **kwargs):

        self._full_file_read = False
        self._record_buffer = []

        super(Pco2wAbcParser, self).__init__(config,
                                             stream_handle,
                                             None,  # State no longer used
                                             None,  # Sieve function no longer used
                                             state_callback,
                                             publish_callback,
                                             exception_callback)

    @staticmethod
    def _create_empty_metadata_dict():

        metadata_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                       Pco2wAbcDataParticleKey.RECORD_TIME,
                                       Pco2wAbcDataParticleKey.CLOCK_ACTIVE,
                                       Pco2wAbcDataParticleKey.RECORDING_ACTIVE,
                                       Pco2wAbcDataParticleKey.RECORD_END_ON_TIME,
                                       Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL,
                                       Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR,
                                       Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK,
                                       Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK,
                                       Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL,
                                       Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                                       Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                                       Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                                       Pco2wAbcDataParticleKey.FLASH_ERASED,
                                       Pco2wAbcDataParticleKey.POWER_ON_INVALID,
                                       Pco2wAbcDataParticleKey.NUM_DATA_RECORDS,
                                       Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS,
                                       Pco2wAbcDataParticleKey.NUM_BYTES_STORED,
                                       Pco2wAbcDataParticleKey.VOLTAGE_BATTERY],
                                      None)

        return metadata_dict

    @staticmethod
    def _create_empty_power_dict():

        power_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                    Pco2wAbcDataParticleKey.RECORD_TIME],
                                   None)

        return power_dict

    @staticmethod
    def _create_empty_instrument_dict():

        instrument_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                         Pco2wAbcDataParticleKey.RECORD_TIME,
                                         Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS,
                                         Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                                         Pco2wAbcDataParticleKey.THERMISTOR_RAW],
                                        None)

        return instrument_dict

    @staticmethod
    def _create_empty_instrument_blank_dict():

        instrument_dict = dict.fromkeys([Pco2wAbcDataParticleKey.RECORD_TYPE,
                                         Pco2wAbcDataParticleKey.RECORD_TIME,
                                         Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS,
                                         Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                                         Pco2wAbcDataParticleKey.THERMISTOR_RAW],
                                        None)

        return instrument_dict

    @staticmethod
    def _fill_metadata_dict(line, metadata_dict):

        metadata_match = METADATA_MATCHER.match(line)

        if metadata_match:
            metadata_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
                metadata_match.group(RECORD_TYPE_GROUP_INDEX)
            metadata_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
                metadata_match.group(RECORD_TIME_GROUP_INDEX)

            flags = format(int(metadata_match.group(FLAGS_GROUP_INDEX)), '016b')

            metadata_dict[Pco2wAbcDataParticleKey.CLOCK_ACTIVE] = \
                flags[CLOCK_ACTIVE_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.RECORDING_ACTIVE] = \
                flags[RECORDING_ACTIVE_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.RECORD_END_ON_TIME] = \
                flags[RECORD_END_ON_TIME_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL] = \
                flags[RECORD_MEMORY_FULL_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR] = \
                flags[RECORD_END_ON_ERROR_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK] = \
                flags[DATA_DOWNLOAD_OK_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN] = \
                flags[FLASH_MEMORY_OPEN_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART] = \
                flags[BATTERY_LOW_PRESTART_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT] = \
                flags[BATTERY_LOW_MEASUREMENT_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK] = \
                flags[BATTERY_LOW_BLANK_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL] = \
                flags[BATTERY_LOW_EXTERNAL_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT] = \
                flags[EXTERNAL_DEVICE1_FAULT_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT] = \
                flags[EXTERNAL_DEVICE2_FAULT_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT] = \
                flags[EXTERNAL_DEVICE3_FAULT_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.FLASH_ERASED] = \
                flags[FLASH_ERASED_FLAGS_INDEX]
            metadata_dict[Pco2wAbcDataParticleKey.POWER_ON_INVALID] = \
                flags[POWER_ON_INVALID_FLAGS_INDEX]

            metadata_dict[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS] = \
                metadata_match.group(NUM_DATA_RECORDS_GROUP_INDEX)
            metadata_dict[Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS] = \
                metadata_match.group(NUM_ERROR_RECORDS_GROUP_INDEX)
            metadata_dict[Pco2wAbcDataParticleKey.NUM_BYTES_STORED] = \
                metadata_match.group(NUM_BYTES_STORED_GROUP_INDEX)

    @staticmethod
    def _fill_power_dict(line, power_dict):

        power_match = POWER_MATCHER.match(line)

        if power_match:
            power_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
                power_match.group(RECORD_TYPE_GROUP_INDEX)
            power_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
                power_match.group(RECORD_TIME_GROUP_INDEX)

    @staticmethod
    def _fill_instrument_dict(line, instrument_dict):
        instrument_record_match = INSTRUMENT_MATCHER.match(line)

        if instrument_record_match:
            instrument_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
                instrument_record_match.group(RECORD_TYPE_GROUP_INDEX)
            instrument_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
                instrument_record_match.group(RECORD_TIME_GROUP_INDEX)
            instrument_dict[Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS] = \
                instrument_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX).split('\t')
            instrument_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
                instrument_record_match.group(BATTERY_VOLTAGE_GROUP_INDEX)
            instrument_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = \
                instrument_record_match.group(THERMISTOR_GROUP_INDEX)

    @staticmethod
    def _fill_instrument_blank_dict(line, instrument_blank_dict):
        instrument_blank_record_match = INSTRUMENT_BLANK_MATCHER.match(line)

        if instrument_blank_record_match:
            instrument_blank_dict[Pco2wAbcDataParticleKey.RECORD_TYPE] = \
                instrument_blank_record_match.group(RECORD_TYPE_GROUP_INDEX)
            instrument_blank_dict[Pco2wAbcDataParticleKey.RECORD_TIME] = \
                instrument_blank_record_match.group(RECORD_TIME_GROUP_INDEX)
            instrument_blank_dict[Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS] = \
                instrument_blank_record_match.group(LIGHT_MEASUREMENTS_GROUP_INDEX). \
                rstrip().split('\t')
            instrument_blank_dict[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY] = \
                instrument_blank_record_match.group(BATTERY_VOLTAGE_GROUP_INDEX)
            instrument_blank_dict[Pco2wAbcDataParticleKey.THERMISTOR_RAW] = \
                instrument_blank_record_match.group(THERMISTOR_GROUP_INDEX)

    def parse_file(self):

        metadata_dict = self._create_empty_metadata_dict()
        power_dict = self._create_empty_power_dict()
        instrument_dict = self._create_empty_instrument_dict()
        instrument_blank_dict = self._create_empty_instrument_blank_dict()

        line = self._stream_handle.readline()

        while line:

            if None in metadata_dict.values():

                self._fill_metadata_dict(line, metadata_dict)

            else:
                timestamp = float(ntplib.system_to_ntp_time(time.time()))
                particle = self._extract_sample(Pco2wAbcMetadataDataParticle,
                                                None,
                                                metadata_dict,
                                                timestamp)

                self._record_buffer.append(particle)

                metadata_dict = self._create_empty_metadata_dict()

            if None in power_dict.values():
                self._fill_power_dict(line, power_dict)

            else:
                timestamp = float(ntplib.system_to_ntp_time(time.time()))

                particle = self._extract_sample(Pco2wAbcPowerDataParticle,
                                                None,
                                                power_dict,
                                                timestamp)

                self._record_buffer.append(particle)

                power_dict = self._create_empty_power_dict()

            if None in instrument_dict.values():

                self._fill_instrument_dict(line, instrument_dict)

                instrument_dict = self._create_empty_instrument_dict()

            else:
                timestamp = float(ntplib.system_to_ntp_time(time.time()))

                particle = self._extract_sample(Pco2wAbcInstrumentDataParticle,
                                                None,
                                                instrument_dict,
                                                timestamp)

                self._record_buffer.append(particle)

                power_dict = self._create_empty_instrument_dict()

            if None in instrument_blank_dict.values():

                self._fill_instrument_blank_dict(line, instrument_blank_dict)

                instrument_blank_dict = self._create_empty_instrument_blank_dict()

            else:
                timestamp = float(ntplib.system_to_ntp_time(time.time()))

                particle = self._extract_sample(Pco2wAbcInstrumentBlankDataParticle,
                                                None,
                                                instrument_blank_dict,
                                                timestamp)

                self._record_buffer.append(particle)

                power_dict = self._create_empty_instrument_blank_dict()

            line = self._stream_handle.readline()

        self._full_file_read = True

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested > 0:

            print "MCW0"

            if self._full_file_read is False:
                self.parse_file()

            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop())

        return particles_to_return
