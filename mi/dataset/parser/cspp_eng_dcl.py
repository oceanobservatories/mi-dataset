#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/cspp_eng_dcl.py
@author Jeff Roy
@brief Parser for CSPP Engineering data collected by acoustic modem
Release notes:

initial release
"""

import re

from mi.core.log import get_logger
from mi.core.instrument.data_particle import DataParticle
from mi.core.common import BaseEnum

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, ANY_CHARS_REGEX, \
    ASCII_HEX_CHAR_REGEX, FLOAT_REGEX
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_ntp_time

log = get_logger()

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

MODE_REGEX = r'(?P<mode>(sent|rcvd)):'
TIMESTAMP_REGEX = r'(?P<timestamp>' + DATE_YYYY_MM_DD_REGEX
TIMESTAMP_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
TIMESTAMP_REGEX += TIME_HR_MIN_SEC_MSEC_REGEX + ')'
MESSAGE_REGEX = r'(?P<message>' + ANY_CHARS_REGEX + ')'

RECORD_REGEX = TIMESTAMP_REGEX
RECORD_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
RECORD_REGEX += MODE_REGEX
RECORD_REGEX += ONE_OR_MORE_WHITESPACE_REGEX
RECORD_REGEX += MESSAGE_REGEX
RECORD_REGEX += END_OF_LINE_REGEX

RECORD_MATCHER = re.compile(RECORD_REGEX)

NMEA_REGEX = r'(?P<sentence>\$PWETA'
NMEA_REGEX += ANY_CHARS_REGEX + ')'
NMEA_REGEX += r'\*(?P<checksum>' + ASCII_HEX_CHAR_REGEX + r'{2})'

NMEA_MATCHER = re.compile(NMEA_REGEX)

RANGE_REGEX = r'Range 1 to 2 :' + ONE_OR_MORE_WHITESPACE_REGEX
RANGE_REGEX += r'(?P<range>' + FLOAT_REGEX
RANGE_REGEX += r')' + ONE_OR_MORE_WHITESPACE_REGEX + 'm'
RANGE_MATCHER = re.compile(RANGE_REGEX)

DSP_REGEX = r'DSP Bat =' + ONE_OR_MORE_WHITESPACE_REGEX
DSP_REGEX += r'(?P<dsp_bat>' + FLOAT_REGEX + r')V'
DSP_MATCHER = re.compile(DSP_REGEX)

XMIT_REGEX = r'Xmit Bat =' + ONE_OR_MORE_WHITESPACE_REGEX
XMIT_REGEX += r'(?P<dsp_bat>' + FLOAT_REGEX + r')V'
XMIT_MATCHER = re.compile(XMIT_REGEX)


# Define the particle stream names
class CsppEngDclParticleType(BaseEnum):
    MODEM = 'cspp_eng_dcl_modem'
    DATE = 'cspp_eng_dcl_date'
    STATE = 'cspp_eng_dcl_state'
    START = 'cspp_eng_dcl_start'
    SUMMARY = 'cspp_eng_dcl_summary'
    ENABLE = 'cspp_eng_dcl_enable'
    WAVE_EST = 'cspp_eng_dcl_wave_estimate'
    PROFILE = 'cspp_eng_dcl_profile'


class CsppEngDclParticle(DataParticle):

    _param_map = None  # Must be defined in subclass

    def _build_parsed_values(self):

        if self._param_map is None:
            raise NotImplemented('self._param_map not defined')

        return [self._encode_value(name, data, function)
                for (name, function), data in zip(self._param_map, self.raw_data)]


class CsppEngDclModemParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.MODEM

    _param_map = [('cspp_modem_distance', float),
                  ('cspp_dsp_battery_voltage', float),
                  ('cspp_transmit_battery_voltage', float)
                  ]


class CsppEngDclDateParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.DATE

    _param_map = [('cspp_date', str),
                  ('cspp_time', str),
                  ('cspp_time_zone', float)
                  ]


class CsppEngDclStateParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.STATE

    _param_map = [('cspp_state', int)]


class CsppEngDclStartParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.START

    _param_map = [('cspp_profile_start_time', str),
                  ('cspp_profile_start_date', str),
                  ('cspp_profile_start_seconds', float)
                  ]


class CsppEngDclSummaryParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.SUMMARY

    _param_map = [('cspp_start_depth', float),
                  ('cspp_end_depth', float),
                  ('cspp_travel_time', float),
                  ('cspp_system_voltage', float),
                  ('cspp_winch_end_position', int),
                  ('cspp_filename', str),
                  ('cspp_file_size', int),
                  ('cspp_file_space', int),
                  ('cspp_num_files', int),
                  ('cspp_profile_number', int),
                  ('cspp_low_voltage', float),
                  ('cspp_low_cell_voltage', float),
                  ('cspp_watts_used', float),
                  ('cspp_low_battery_number', int)
                  ]


class CsppEngDclEnableParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.ENABLE

    _param_map = [('cspp_enable_device ', str),
                  ('cspp_decimation_factor', int),
                  ]


class CsppEngDclWaveEstParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.WAVE_EST

    _param_map = [('cspp_wave_start_time', str),
                  ('cspp_wave_start_date', str),
                  ('cspp_wave_height', float),
                  ('cspp_wave_period', float),
                  ('cspp_wave_mode', int),
                  ]


class CsppEngDclProfileParticle(CsppEngDclParticle):

    _data_particle_type = CsppEngDclParticleType.PROFILE

    _param_map = [('cspp_home_depth', float),
                  ('cspp_start_time', str),
                  ('cspp_setup_rate', float),
                  ('cspp_setup_start_depth', float),
                  ('cspp_setup_end_depth', float),
                  ('cspp_setup_ascent_rate', float),
                  ('cspp_setup_descent_rate', float),
                  ('cspp_num_profiles', int),
                  ('cspp_interval_time', int),
                  ('cspp_start_delay', int),
                  ]


# dictionary mapping expected NMEA commands to number of expected fields and corresponding particle class
NMEA_SENTENCE_MAP = {'DATE': (3, CsppEngDclDateParticle),
                     'PFS': (2, CsppEngDclStateParticle),
                     'PST': (3, CsppEngDclStartParticle),
                     'SUM': (14, CsppEngDclSummaryParticle),
                     'ENA': (2, CsppEngDclEnableParticle),
                     'WHE': (5, CsppEngDclWaveEstParticle),
                     'PRO': (10, CsppEngDclProfileParticle)
                     }


class CsppEngDclParser(SimpleParser):

    @staticmethod
    def calc_checksum(sentence):
        """   Calculates NMEA checksum

        :param sentence:
        :return: string of checksum in upper case hex notation
        """
        checksum = 0
        for c in sentence:
            checksum ^= ord(c)

        return str.upper(hex(checksum))

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        #  initialize data for modem particle
        first_timestamp = None
        distance = None
        dsp_bat = None
        xmit_bat = None

        for line in self._stream_handle:

            data_match = RECORD_MATCHER.match(line)
            # skip over the sent messages
            if data_match.group('mode') == 'send':
                continue  # go to next line

            timestamp_str = data_match.group('timestamp')
            message = data_match.group('message')

            if first_timestamp is None:
                first_timestamp = timestamp_str  # save the first timestamp for the modem particle

            # save off header information for modem particle
            # modem particle created after processing entire file.
            range_match = RANGE_MATCHER.match(message)
            if range_match:
                distance = range_match.group('range')
                continue  # go to next line
            dsp_match = DSP_MATCHER.match(message)
            if dsp_match:
                dsp_bat = dsp_match.group('dsp_bat')
                continue  # go to next line
            xmit_match = XMIT_MATCHER.match(message)
            if xmit_match:
                xmit_bat = xmit_match.group('dsp_bat')
                continue  # go to next line

            # process NMEA sentences
            nmea_match = NMEA_MATCHER.match(message)
            if nmea_match:
                sentence = nmea_match.group('sentence')
                checksum = '0X' + nmea_match.group('checksum')  # prepend hex prefix

                # Note: NMEA checksums typically do not include the $ at the
                # beginning of the sentence but it appears Wetlabs implemented
                # it that way.
                comp_checksum = self.calc_checksum(sentence)

                if comp_checksum == checksum:
                    fields = sentence.split(',')
                    command = fields[5]
                    count = fields[6]

                    sentence_params = NMEA_SENTENCE_MAP.get(command)

                    if sentence_params is None:
                        # skip NMEA sentences we are not looking for
                        log.info('NMEA sentence skipped %s', line)
                        continue  # go to next line

                    expected_count, particle_class = sentence_params
                    if int(count) == expected_count:

                        # Create particle and add to buffer
                        timestamp = dcl_controller_timestamp_to_ntp_time(timestamp_str)
                        data_particle = self._extract_sample(particle_class,
                                                             None,
                                                             fields[7:],
                                                             timestamp)
                        self._record_buffer.append(data_particle)
                    else:
                        log.warn('did not get expected number of fields on line %s', line)
                else:
                    log.warn('checksum failed on line %s', line)

        # only send modem particle if we have a timestamp
        # and at least one parameter
        if first_timestamp and (distance or dsp_bat or xmit_bat):
            timestamp = dcl_controller_timestamp_to_ntp_time(first_timestamp)
            data_particle = self._extract_sample(CsppEngDclModemParticle,
                                                 None,
                                                 [distance, dsp_bat, xmit_bat],
                                                 timestamp)
            self._record_buffer.append(data_particle)

