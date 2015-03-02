#!/usr/bin/env python

"""
@package mi.dataset.parser.adcp_pd0
@file marine-integrations/mi/dataset/parser/adcp_pd0.py
@author Jeff Roy
@brief Parser for the adcps_jln and moas_gl_adcpa dataset drivers
Release notes:

initial release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

import copy
import datetime as dt
import ntplib
import re
import struct

from calendar import timegm

from mi.core.log import get_logger

log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import \
    DataParticle, DataParticleKey, DataParticleValue

from mi.core.exceptions import SampleException, \
    UnexpectedDataException, \
    RecoverableSampleException, \
    ConfigurationException

from mi.dataset.dataset_parser import SimpleParser

ADCPS_PD0_HEADER_REGEX = b'\x7f\x7f'  # header bytes in PD0 files flagged by 7F7F


# define the lengths of ensemble parts
FIXED_HEADER_BYTES = 6
NUM_BYTES_BYTES = 2  # The number of bytes for the number of bytes field.
OFFSET_BYTES = 2
ID_BYTES = 2

ADCPS_FIXED_LEADER_BYTES = 59
ADCPA_FIXED_LEADER_BYTES = 58
ADCPA_AUV_FIXED_LEADER_BYTES = 59

ADCPS_VARIABLE_LEADER_BYTES = 65
ADCPA_VARIABLE_LEADER_BYTES = 60
ADCPA_AUV_VARIABLE_LEADER_BYTES = 46

VELOCITY_BYTES_PER_CELL = 8
CORRELATION_BYTES_PER_CELL = 4
ECHO_INTENSITY_BYTES_PER_CELL = 4
PERCENT_GOOD_BYTES_PER_CELL = 4

ADCPS_BOTTOM_TRACK_BYTES = 85
ADCPA_BOTTOM_TRACK_BYTES = 81
ADCPA_AUV_BOTTOM_TRACK_BYTES = 81

CHECKSUM_BYTES = 2

# used to verify 16 bit checksum
CHECKSUM_MODULO = 65535

# IDs of the different parts of the ensemble.
FIXED_LEADER_ID = 0  # 00 00x
VARIABLE_LEADER_ID = 128  # 00 80x
VELOCITY_ID = 256  # 01 00x
CORRELATION_ID = 512  # 02 00x
ECHO_INTENSITY_ID = 768  # 03 00x
PERCENT_GOOD_ID = 1024  # 04 00x
BOTTOM_TRACK_ID = 1536  # 06 00x
AUV_NAV_DATA_ID = 8192  # 20 00x
STATUS_DATA_ID = 1280  # 05 00x


class AdcpPd0ParserDataParticleKey(BaseEnum):
    """
    Data particles for the Teledyne ADCPs Workhorse PD0 formatted data files
    """

    # # Header Data
    # HEADER_ID = 'header_id'
    # DATA_SOURCE_ID = 'data_source_id'
    # NUM_BYTES = 'num_bytes'
    # NUM_DATA_TYPES = 'num_data_types'
    # OFFSET_DATA_TYPES = 'offset_data_types'
    #
    # # Fixed Leader Data
    # FIXED_LEADER_ID = 'fixed_leader_id'

    FIRMWARE_VERSION = 'firmware_version'
    FIRMWARE_REVISION = 'firmware_revision'
    SYSCONFIG_FREQUENCY = 'sysconfig_frequency'
    SYSCONFIG_BEAM_PATTERN = 'sysconfig_beam_pattern'
    SYSCONFIG_SENSOR_CONFIG = 'sysconfig_sensor_config'
    SYSCONFIG_HEAD_ATTACHED = 'sysconfig_head_attached'
    SYSCONFIG_VERTICAL_ORIENTATION = 'sysconfig_vertical_orientation'
    SYSCONFIG_BEAM_ANGLE = 'sysconfig_beam_angle'
    SYSCONFIG_BEAM_CONFIG = 'sysconfig_beam_config'
    DATA_FLAG = 'data_flag'
    LAG_LENGTH = 'lag_length'
    NUM_BEAMS = 'num_beams'
    NUM_CELLS = 'num_cells'
    PINGS_PER_ENSEMBLE = 'pings_per_ensemble'
    DEPTH_CELL_LENGTH = 'cell_length'
    BLANK_AFTER_TRANSMIT = 'blank_after_transmit'
    SIGNAL_PROCESSING_MODE = 'signal_processing_mode'
    LOW_CORR_THRESHOLD = 'low_corr_threshold'
    NUM_CODE_REPETITIONS = 'num_code_repetitions'
    PERCENT_GOOD_MIN = 'percent_good_min'
    ERROR_VEL_THRESHOLD = 'error_vel_threshold'
    TIME_PER_PING_MINUTES = 'time_per_ping_minutes'
    TIME_PER_PING_SECONDS = 'time_per_ping_seconds'
    COORD_TRANSFORM_TYPE = 'coord_transform_type'
    COORD_TRANSFORM_TILTS = 'coord_transform_tilts'
    COORD_TRANSFORM_BEAMS = 'coord_transform_beams'
    COORD_TRANSFORM_MAPPING = 'coord_transform_mapping'
    HEADING_ALIGNMENT = 'heading_alignment'
    HEADING_BIAS = 'heading_bias'
    SENSOR_SOURCE_SPEED = 'sensor_source_speed'
    SENSOR_SOURCE_DEPTH = 'sensor_source_depth'
    SENSOR_SOURCE_HEADING = 'sensor_source_heading'
    SENSOR_SOURCE_PITCH = 'sensor_source_pitch'
    SENSOR_SOURCE_ROLL = 'sensor_source_roll'
    SENSOR_SOURCE_CONDUCTIVITY = 'sensor_source_conductivity'
    SENSOR_SOURCE_TEMPERATURE = 'sensor_source_temperature'
    SENSOR_SOURCE_TEMPERATURE_EU = 'sensor_source_temperature_eu'  # ADCPA Only
    SENSOR_AVAILABLE_SPEED = 'sensor_available_speed'
    SENSOR_AVAILABLE_DEPTH = 'sensor_available_depth'
    SENSOR_AVAILABLE_HEADING = 'sensor_available_heading'
    SENSOR_AVAILABLE_PITCH = 'sensor_available_pitch'
    SENSOR_AVAILABLE_ROLL = 'sensor_available_roll'
    SENSOR_AVAILABLE_CONDUCTIVITY = 'sensor_available_conductivity'
    SENSOR_AVAILABLE_TEMPERATURE = 'sensor_available_temperature'
    SENSOR_AVAILABLE_TEMPERATURE_EU = 'sensor_available_temperature_eu'  # ADCPA Only
    BIN_1_DISTANCE = 'bin_1_distance'
    TRANSMIT_PULSE_LENGTH = 'transmit_pulse_length'
    REFERENCE_LAYER_START = 'reference_layer_start'
    REFERENCE_LAYER_STOP = 'reference_layer_stop'
    FALSE_TARGET_THRESHOLD = 'false_target_threshold'
    LOW_LATENCY_TRIGGER = 'low_latency_trigger'
    TRANSMIT_LAG_DISTANCE = 'transmit_lag_distance'
    CPU_SERIAL_NUM = 'cpu_board_serial_number'  # ADCPS Only
    SYSTEM_BANDWIDTH = 'system_bandwidth'  # ADCPS & ADCPA (glider) Only
    SYSTEM_POWER = 'system_power'  # ADCPS Only
    SERIAL_NUMBER = 'serial_number'
    BEAM_ANGLE = 'beam_angle'  # ADCPS & ADCPA AUV Only

    # Variable Leader Data
    # VARIABLE_LEADER_ID = 'variable_leader_id'
    ENSEMBLE_NUMBER = 'ensemble_number'
    REAL_TIME_CLOCK = 'real_time_clock'
    ENSEMBLE_START_TIME = 'ensemble_start_time'
    ENSEMBLE_NUMBER_INCREMENT = 'ensemble_number_increment'
    BIT_RESULT_DEMOD_1 = 'bit_result_demod_1'  # ADCPS & ADCPA AUV Only
    BIT_RESULT_DEMOD_0 = 'bit_result_demod_0'  # ADCPS & ADCPA AUV Only
    BIT_RESULT_TIMING = 'bit_result_timing'  # ADCPS & ADCPA AUV Only
    BIT_ERROR_NUMBER = 'bit_error_number'  # ADCPA (glider) Only
    BIT_ERROR_COUNT = 'bit_error_count'    # ADCPA (glider) Only
    SPEED_OF_SOUND = 'speed_of_sound'
    TRANSDUCER_DEPTH = 'transducer_depth'
    HEADING = 'heading'
    PITCH = 'pitch'
    ROLL = 'roll'
    SALINITY = 'salinity'
    TEMPERATURE = 'temperature'
    MPT_MINUTES = 'mpt_minutes'
    MPT_SECONDS = 'mpt_seconds'
    HEADING_STDEV = 'heading_stdev'
    PITCH_STDEV = 'pitch_stdev'
    ROLL_STDEV = 'roll_stdev'
    ADC_TRANSMIT_CURRENT = 'adc_transmit_current'  # ADCPS & ADCPA AUV Only
    ADC_TRANSMIT_VOLTAGE = 'adc_transmit_voltage'  # ADCPS & ADCPA AUV Only
    ADC_AMBIENT_TEMP = 'adc_ambient_temp'  # ADCPS & ADCPA AUV Only
    ADC_PRESSURE_PLUS = 'adc_pressure_plus'  # ADCPS & ADCPA AUV Only
    ADC_PRESSURE_MINUS = 'adc_pressure_minus'  # ADCPS & ADCPA AUV Only
    ADC_ATTITUDE_TEMP = 'adc_attitude_temp'  # ADCPS & ADCPA AUV Only
    ADC_ATTITUDE = 'adc_attitude'  # ADCPS & ADCPA AUV Only
    ADC_CONTAMINATION_SENSOR = 'adc_contamination_sensor'  # ADCPS & ADCPA AUV Only
    BUS_ERROR_EXCEPTION = 'bus_error_exception'  # ADCPS & ADCPA AUV Only
    ADDRESS_ERROR_EXCEPTION = 'address_error_exception'  # ADCPS & ADCPA AUV Only
    ILLEGAL_INSTRUCTION_EXCEPTION = 'illegal_instruction_exception'  # ADCPS & ADCPA AUV Only
    ZERO_DIVIDE_INSTRUCTION = 'zero_divide_instruction'  # ADCPS & ADCPA AUV Only
    EMULATOR_EXCEPTION = 'emulator_exception'  # ADCPS & ADCPA AUV Only
    UNASSIGNED_EXCEPTION = 'unassigned_exception'  # ADCPS & ADCPA AUV Only
    WATCHDOG_RESTART_OCCURRED = 'watchdog_restart_occurred'  # ADCPS Only
    BATTERY_SAVER_POWER = 'battery_saver_power'  # ADCPS Only
    PINGING = 'pinging'  # ADCPS & ADCPA AUV Only
    COLD_WAKEUP_OCCURRED = 'cold_wakeup_occurred'  # ADCPS & ADCPA AUV Only
    UNKNOWN_WAKEUP_OCCURRED = 'unknown_wakeup_occurred'  # ADCPS & ADCPA AUV Only
    CLOCK_READ_ERROR = 'clock_read_error'  # ADCPS & ADCPA AUV Only
    UNEXPECTED_ALARM = 'unexpected_alarm'  # ADCPS Only
    CLOCK_JUMP_FORWARD = 'clock_jump_forward'  # ADCPS Only
    CLOCK_JUMP_BACKWARD = 'clock_jump_backward'  # ADCPS Only
    POWER_FAIL = 'power_fail'  # ADCPS & ADCPA AUV Only (note different bit in each variant)
    SPURIOUS_DSP_INTERRUPT = 'spurious_dsp_interrupt'  # ADCPS Only
    SPURIOUS_UART_INTERRUPT = 'spurious_uart_interrupt'  # ADCPS & ADCPA AUV Only
    SPURIOUS_CLOCK_INTERRUPT = 'spurious_clock_interrupt'  # ADCPS & ADCPA AUV Only
    LEVEL_7_INTERRUPT = 'level_7_interrupt'  # ADCPS Only
    PRESSURE = 'pressure'  # ADCPS and ADCPA (glider) Only
    PRESSURE_VARIANCE = 'pressure_variance'  # ADCPS and ADCPA (glider) Only

    REAL_TIME_CLOCK2 = 'real_time_clock_2'  # ADCPS Only
    ENSEMBLE_START_TIME2 = 'ensemble_start_time_2'  # ADCPS Only

    # Velocity Data
    # VELOCITY_DATA_ID = 'velocity_data_id'
    WATER_VELOCITY_EAST = 'water_velocity_east'  # ADCPS and ADCPA (glider) Only
    WATER_VELOCITY_NORTH = 'water_velocity_north'  # ADCPS and ADCPA (glider) Only
    WATER_VELOCITY_UP = 'water_velocity_up'  # ADCPS and ADCPA (glider) Only
    WATER_VELOCITY_FORWARD = 'water_velocity_forward'  # ADCPA AUV Only
    WATER_VELOCITY_STARBOARD = 'water_velocity_starboard'  # ADCPA AUV Only
    WATER_VELOCITY_VERTICAL = 'water_velocity_vertical'  # ADCPA AUV Only
    ERROR_VELOCITY = 'error_velocity'

    # Correlation Magnitude Data
    # CORRELATION_MAGNITUDE_ID = 'correlation_magnitude_id'
    CORRELATION_MAGNITUDE_BEAM1 = 'correlation_magnitude_beam1'
    CORRELATION_MAGNITUDE_BEAM2 = 'correlation_magnitude_beam2'
    CORRELATION_MAGNITUDE_BEAM3 = 'correlation_magnitude_beam3'
    CORRELATION_MAGNITUDE_BEAM4 = 'correlation_magnitude_beam4'

    # Echo Intensity Data
    # ECHO_INTENSITY_ID = 'echo_intensity_id'
    ECHO_INTENSITY_BEAM1 = 'echo_intensity_beam1'
    ECHO_INTENSITY_BEAM2 = 'echo_intensity_beam2'
    ECHO_INTENSITY_BEAM3 = 'echo_intensity_beam3'
    ECHO_INTENSITY_BEAM4 = 'echo_intensity_beam4'

    # Percent Good Data
    # PERCENT_GOOD_ID = 'percent_good_id'
    PERCENT_GOOD_3BEAM = 'percent_good_3beam'
    PERCENT_TRANSFORMS_REJECT = 'percent_transforms_reject'
    PERCENT_BAD_BEAMS = 'percent_bad_beams'
    PERCENT_GOOD_4BEAM = 'percent_good_4beam'

    # Bottom Track Data (only produced for ADCPA
    # when the glider is in less than 65 m of water)
    # BOTTOM_TRACK_ID = 'bottom_track_id'
    BT_PINGS_PER_ENSEMBLE = 'bt_pings_per_ensemble'
    BT_DELAY_BEFORE_REACQUIRE = 'bt_delay_before_reacquire'
    BT_CORR_MAGNITUDE_MIN = 'bt_corr_magnitude_min'
    BT_EVAL_MAGNITUDE_MIN = 'bt_eval_magnitude_min'
    BT_PERCENT_GOOD_MIN = 'bt_percent_good_min'
    BT_MODE = 'bt_mode'
    BT_ERROR_VELOCITY_MAX = 'bt_error_velocity_max'

    BT_BEAM1_RANGE = 'bt_beam1_range'
    BT_BEAM2_RANGE = 'bt_beam2_range'
    BT_BEAM3_RANGE = 'bt_beam3_range'
    BT_BEAM4_RANGE = 'bt_beam4_range'

    BT_EASTWARD_VELOCITY = 'bt_eastward_velocity'  # ADCPS and ADCPA (glider) Only
    BT_NORTHWARD_VELOCITY = 'bt_northward_velocity'  # ADCPS and ADCPA (glider) Only
    BT_UPWARD_VELOCITY = 'bt_upward_velocity'  # ADCPS and ADCPA (glider) Only
    BT_FORWARD_VELOCITY = 'bt_forward_velocity'  # ADCPA AUV Only
    BT_STARBOARD_VELOCITY = 'bt_starboard_velocity'  # ADCPA AUV Only
    BT_VERTICAL_VELOCITY = 'bt_vertical_velocity'  # ADCPA AUV Only
    BT_ERROR_VELOCITY = 'bt_error_velocity'
    BT_BEAM1_CORRELATION = 'bt_beam1_correlation'
    BT_BEAM2_CORRELATION = 'bt_beam2_correlation'
    BT_BEAM3_CORRELATION = 'bt_beam3_correlation'
    BT_BEAM4_CORRELATION = 'bt_beam4_correlation'
    BT_BEAM1_EVAL_AMP = 'bt_beam1_eval_amp'
    BT_BEAM2_EVAL_AMP = 'bt_beam2_eval_amp'
    BT_BEAM3_EVAL_AMP = 'bt_beam3_eval_amp'
    BT_BEAM4_EVAL_AMP = 'bt_beam4_eval_amp'
    BT_BEAM1_PERCENT_GOOD = 'bt_beam1_percent_good'
    BT_BEAM2_PERCENT_GOOD = 'bt_beam2_percent_good'
    BT_BEAM3_PERCENT_GOOD = 'bt_beam3_percent_good'
    BT_BEAM4_PERCENT_GOOD = 'bt_beam4_percent_good'
    BT_REF_LAYER_MIN = 'bt_ref_layer_min'
    BT_REF_LAYER_NEAR = 'bt_ref_layer_near'
    BT_REF_LAYER_FAR = 'bt_ref_layer_far'
    BT_EASTWARD_REF_LAYER_VELOCITY = 'bt_eastward_ref_layer_velocity'  # ADCPS and ADCPA (glider) Only
    BT_NORTHWARD_REF_LAYER_VELOCITY = 'bt_northward_ref_layer_velocity'  # ADCPS and ADCPA (glider) Only
    BT_UPWARD_REF_LAYER_VELOCITY = 'bt_upward_ref_layer_velocity'  # ADCPS and ADCPA (glider) Only
    BT_FORWARD_REF_LAYER_VELOCITY = 'bt_forward_ref_layer_velocity'  # ADCPA AUV Only
    BT_STARBOARD_REF_LAYER_VELOCITY = 'bt_starboard_ref_layer_velocity'  # ADCPA AUV Only
    BT_VERTICAL_REF_LAYER_VELOCITY = 'bt_vertical_ref_layer_velocity'  # ADCPA AUV Only
    BT_ERROR_REF_LAYER_VELOCITY = 'bt_error_ref_layer_velocity'
    BT_BEAM1_REF_CORRELATION = 'bt_beam1_ref_correlation'
    BT_BEAM2_REF_CORRELATION = 'bt_beam2_ref_correlation'
    BT_BEAM3_REF_CORRELATION = 'bt_beam3_ref_correlation'
    BT_BEAM4_REF_CORRELATION = 'bt_beam4_ref_correlation'
    BT_BEAM1_REF_INTENSITY = 'bt_beam1_ref_intensity'
    BT_BEAM2_REF_INTENSITY = 'bt_beam2_ref_intensity'
    BT_BEAM3_REF_INTENSITY = 'bt_beam3_ref_intensity'
    BT_BEAM4_REF_INTENSITY = 'bt_beam4_ref_intensity'
    BT_BEAM1_REF_PERCENT_GOOD = 'bt_beam1_ref_percent_good'
    BT_BEAM2_REF_PERCENT_GOOD = 'bt_beam2_ref_percent_good'
    BT_BEAM3_REF_PERCENT_GOOD = 'bt_beam3_ref_percent_good'
    BT_BEAM4_REF_PERCENT_GOOD = 'bt_beam4_ref_percent_good'
    BT_MAX_DEPTH = 'bt_max_depth'
    BT_BEAM1_RSSI_AMPLITUDE = 'bt_beam1_rssi_amplitude'
    BT_BEAM2_RSSI_AMPLITUDE = 'bt_beam2_rssi_amplitude'
    BT_BEAM3_RSSI_AMPLITUDE = 'bt_beam3_rssi_amplitude'
    BT_BEAM4_RSSI_AMPLITUDE = 'bt_beam4_rssi_amplitude'
    BT_GAIN = 'bt_gain'

    # Ensemble checksum
    # CHECKSUM = 'checksum'


class StateKey(BaseEnum):
    POSITION = 'position'  # number of bytes read


class AdcpFileType(BaseEnum):
    # enumeration of the different PD0 file formats
    ADCPA_FILE = 'adcpa_file'  # ADCPA PD0 files are used by the ExplorerDVL instruments found on gliders
    ADCPS_File = 'adcps_file'  # ADCPS(T) PD0 files are used by the Workhorse LongRanger Monitor
    ADCPA_AUV_FILE = 'adcpa_auv_file'  # ADCPA PD0 files are used by Workhorse Navigator found on AUVs


class AdcpPd0DataParticle(DataParticle):
    """
    Intermediate particle class to handle particle streams from PD0 files
    constructor must be passed a valid file_type from the enumeration AdcpFileType
    All other constructor parameters handled by base class
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None,
                 file_type=None):

        self._file_type = file_type
        # file_type must be set to a value in AdcpFileType
        # used for conditional decoding of the raw data in
        # _build_parsed_values

        self.num_depth_cells = None  # initialize # of cells to None

        super(AdcpPd0DataParticle, self).__init__(raw_data,
                                                  port_timestamp,
                                                  internal_timestamp,
                                                  preferred_timestamp,
                                                  quality_flag,
                                                  new_sequence)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        self.final_result = []

        # set particle type specifics
        if self._file_type == AdcpFileType.ADCPA_FILE:
            fixed_leader_bytes = ADCPA_FIXED_LEADER_BYTES
            variable_leader_bytes = ADCPA_VARIABLE_LEADER_BYTES
            bottom_track_bytes = ADCPA_BOTTOM_TRACK_BYTES
        elif self._file_type == AdcpFileType.ADCPS_File:
            fixed_leader_bytes = ADCPS_FIXED_LEADER_BYTES
            variable_leader_bytes = ADCPS_VARIABLE_LEADER_BYTES
            bottom_track_bytes = ADCPS_BOTTOM_TRACK_BYTES
        elif self._file_type == AdcpFileType.ADCPA_AUV_FILE:
            fixed_leader_bytes = ADCPA_AUV_FIXED_LEADER_BYTES
            variable_leader_bytes = ADCPA_AUV_VARIABLE_LEADER_BYTES
            bottom_track_bytes = ADCPA_AUV_BOTTOM_TRACK_BYTES
        else:
            raise ConfigurationException('invalid file type')

        # parse the file header
        (header_id, data_source_id, num_bytes, spare, num_data_types) = \
            struct.unpack_from('<BBHBB', self.raw_data)

        offsets = []  # create list for offsets
        start = FIXED_HEADER_BYTES  # offsets start at byte 6 (using 0 indexing)
        data_types_idx = 1  # counter for n data types
        fixed_leader_found = False

        while data_types_idx <= num_data_types:
            value = struct.unpack_from('<H', self.raw_data, start)[0]

            offsets.append(value)
            start += NUM_BYTES_BYTES
            data_types_idx += 1

        for offset in offsets:
            # for each offset, using the starting byte, determine the data type
            # and then parse accordingly.
            data_type = struct.unpack_from('<H', self.raw_data, offset)[0]

            # fixed leader data (x00x00)
            if data_type == FIXED_LEADER_ID:
                data = self.raw_data[offset:offset + fixed_leader_bytes]
                self.parse_fixed_leader(data)
                fixed_leader_found = True
                num_cells = self.num_depth_cells  # grab the # of depth cells
                # obtained from the fixed leader
                # data type

            # variable leader data (x80x00)
            elif data_type == VARIABLE_LEADER_ID:
                data = self.raw_data[offset:offset + variable_leader_bytes]
                self.parse_variable_leader(data)

            # velocity data (x00x01)
            elif data_type == VELOCITY_ID:

                if not fixed_leader_found:
                    raise SampleException("No Fixed leader")

                # number of bytes is a function of the user selectable number of
                # depth cells (WN command), calculated above
                num_bytes = ID_BYTES + VELOCITY_BYTES_PER_CELL * num_cells
                data = self.raw_data[offset:offset + num_bytes]
                self.parse_velocity_data(data)

            # correlation magnitude data (x00x02)
            elif data_type == CORRELATION_ID:
                if not fixed_leader_found:
                    raise SampleException("No Fixed leader")

                # number of bytes is a function of the user selectable number of
                # depth cells (WN command), calculated above
                num_bytes = ID_BYTES + CORRELATION_BYTES_PER_CELL * num_cells
                data = self.raw_data[offset:offset + num_bytes]
                self.parse_correlation_magnitude_data(data)

            # echo intensity data (x00x03)
            elif data_type == ECHO_INTENSITY_ID:
                if not fixed_leader_found:
                    raise SampleException("No Fixed leader")

                # number of bytes is a function of the user selectable number of
                # depth cells (WN command), calculated above
                num_bytes = ID_BYTES + ECHO_INTENSITY_BYTES_PER_CELL * num_cells
                data = self.raw_data[offset:offset + num_bytes]
                self.parse_echo_intensity_data(data)

            # percent-good data (x00x04)
            elif data_type == PERCENT_GOOD_ID:
                if not fixed_leader_found:
                    raise SampleException("No Fixed leader")

                # number of bytes is a function of the user selectable number of
                # depth cells (WN command), calculated above
                num_bytes = ID_BYTES + PERCENT_GOOD_BYTES_PER_CELL * num_cells
                data = self.raw_data[offset:offset + num_bytes]
                self.parse_percent_good_data(data)

            # bottom track data (x00x06)
            elif data_type == BOTTOM_TRACK_ID:
                if not fixed_leader_found:
                    raise SampleException("No Fixed leader")

                data = self.raw_data[offset:offset + bottom_track_bytes]
                self.parse_bottom_track_data(data)
            elif data_type == AUV_NAV_DATA_ID:
                pass  # expected from AUVs, ignore
            elif data_type == STATUS_DATA_ID:
                pass  # status data is ignored
            else:
                log.warn("Found unrecognizable data type ID in PD0 file. ID(hex) = %.4x ", data_type)
        return self.final_result

    def parse_fixed_leader(self, data):
        """
        Parse the fixed leader portion of the particle
       """
        (fixed_leader_id, firmware_version, firmware_revision,
         sysconfig_lsb, sysconfig_msb, data_flag, lag_length, num_beams, num_cells,
         pings_per_ensemble, depth_cell_length, blank_after_transmit,
         signal_processing_mode, low_corr_threshold, num_code_repetitions,
         percent_good_min, error_vel_threshold, time_per_ping_minutes,
         time_per_ping_seconds, time_per_ping_hundredths, coord_transform_type,
         heading_alignment, heading_bias, sensor_source, sensor_available,
         bin_1_distance, transmit_pulse_length, reference_layer_start,
         reference_layer_stop, false_target_threshold, low_latency_trigger,
         transmit_lag_distance, cpu_serial_num, system_bandwidth,
         system_power, SPARE2, serial_number) = \
            struct.unpack_from('<H8B3H4BH4B2h2B2H4BHQH2BI', data)

        # store the number of depth cells for use elsewhere
        self.num_depth_cells = num_cells

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.FIRMWARE_VERSION,
                                                    firmware_version, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.FIRMWARE_REVISION,
                                                    firmware_revision, int))

        frequencies = [75, 150, 300, 600, 1200, 2400]

        # following items all pulled from the sys config LSB
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_FREQUENCY,
                                                    frequencies[sysconfig_lsb & 0b00000111], int))
        # bitwise and to extract the frequency index
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_BEAM_PATTERN,
                                                    1 if sysconfig_lsb & 0b00001000 else 0, int))
        # bitwise right shift 4 bits note: must do the and first then shift
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_SENSOR_CONFIG,
                                                    (sysconfig_lsb & 0b00110000) >> 4, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_HEAD_ATTACHED,
                                                    1 if sysconfig_lsb & 0b01000000 else 0, int))
        self.final_result.append(
            self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_VERTICAL_ORIENTATION,
                               1 if sysconfig_lsb & 0b10000000 else 0, int))

        # following items all pulled from the sys config MSB
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_BEAM_ANGLE,
                                                    sysconfig_msb & 0b00000011, int))
        # bitwise right shift 4 bits note: must do the and first then shift
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSCONFIG_BEAM_CONFIG,
                                                    (sysconfig_msb & 0b11110000) >> 4, int))

        if 0 != data_flag:
            log.warn("real/sim data_flag was not equal to 0")

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.DATA_FLAG,
                                                    data_flag, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.LAG_LENGTH,
                                                    lag_length, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.NUM_BEAMS,
                                                    num_beams, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.NUM_CELLS,
                                                    num_cells, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PINGS_PER_ENSEMBLE,
                                                    pings_per_ensemble, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.DEPTH_CELL_LENGTH,
                                                    depth_cell_length, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BLANK_AFTER_TRANSMIT,
                                                    blank_after_transmit, int))

        if 1 != signal_processing_mode:
            log.warn("signal_processing_mode was not equal to 1")

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SIGNAL_PROCESSING_MODE,
                                                    signal_processing_mode, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.LOW_CORR_THRESHOLD,
                                                    low_corr_threshold, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.NUM_CODE_REPETITIONS,
                                                    num_code_repetitions, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PERCENT_GOOD_MIN,
                                                    percent_good_min, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ERROR_VEL_THRESHOLD,
                                                    error_vel_threshold, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.TIME_PER_PING_MINUTES,
                                                    time_per_ping_minutes, int))

        tpp_float_seconds = time_per_ping_seconds + (time_per_ping_hundredths / 100.0)
        # combine seconds and hundreds into a float
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.TIME_PER_PING_SECONDS,
                                                    tpp_float_seconds, float))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.COORD_TRANSFORM_TYPE,
                                                    (coord_transform_type & 0b00011000) >> 3, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.COORD_TRANSFORM_TILTS,
                                                    1 if coord_transform_type & 0b00000100 else 0, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.COORD_TRANSFORM_BEAMS,
                                                    1 if coord_transform_type & 0b0000010 else 0, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.COORD_TRANSFORM_MAPPING,
                                                    1 if coord_transform_type & 0b00000001 else 0, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.HEADING_ALIGNMENT,
                                                    heading_alignment, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.HEADING_BIAS,
                                                    heading_bias, int))

        # Note the sensor source and sensor available fields are decoded differently
        # for the different file types
        if self._file_type == AdcpFileType.ADCPS_File or self._file_type == AdcpFileType.ADCPA_AUV_FILE:
            # pull the following out of the sensor source byte
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_SPEED,
                                                        1 if sensor_source & 0b01000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_DEPTH,
                                                        1 if sensor_source & 0b00100000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_HEADING,
                                                        1 if sensor_source & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_PITCH,
                                                        1 if sensor_source & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_ROLL,
                                                        1 if sensor_source & 0b00000100 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_CONDUCTIVITY,
                                                        1 if sensor_source & 0b00000010 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_TEMPERATURE,
                                                        1 if sensor_source & 0b00000001 else 0, int))

            # pull the following out of the sensor available byte
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_SPEED,
                                                        1 if sensor_available & 0b01000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_DEPTH,
                                                        1 if sensor_available & 0b00100000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_HEADING,
                                                        1 if sensor_available & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_PITCH,
                                                        1 if sensor_available & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_ROLL,
                                                        1 if sensor_available & 0b00000100 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_CONDUCTIVITY,
                                                        1 if sensor_available & 0b00000010 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_TEMPERATURE,
                                                        1 if sensor_available & 0b00000001 else 0, int))
            # this is "SPARE" byte in vendor doc, see comments in IDD Record Structure section
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.LOW_LATENCY_TRIGGER,
                                                    low_latency_trigger, int))
            beam_angle = struct.unpack('<B', data[-1:])[0]  # beam angle is last byte
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BEAM_ANGLE,
                                                        beam_angle, int))
        elif self._file_type == AdcpFileType.ADCPA_FILE:  # decoding below is for ADCPA (glider) variants
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSTEM_BANDWIDTH,
                                                        system_bandwidth, int))
            # pull the following out of the sensor source byte
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_SPEED,
                                                        1 if sensor_source & 0b10000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_DEPTH,
                                                        1 if sensor_source & 0b01000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_HEADING,
                                                        1 if sensor_source & 0b00100000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_PITCH,
                                                        1 if sensor_source & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_ROLL,
                                                        1 if sensor_source & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_CONDUCTIVITY,
                                                        1 if sensor_source & 0b00000100 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_TEMPERATURE,
                                                        1 if sensor_source & 0b00000010 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_SOURCE_TEMPERATURE_EU,
                                                        1 if sensor_source & 0b00000001 else 0, int))

            # pull the following out of the sensor available byte
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_SPEED,
                                                        1 if sensor_available & 0b10000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_DEPTH,
                                                        1 if sensor_available & 0b01000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_HEADING,
                                                        1 if sensor_available & 0b00100000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_PITCH,
                                                        1 if sensor_available & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_ROLL,
                                                        1 if sensor_available & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_CONDUCTIVITY,
                                                        1 if sensor_available & 0b00000100 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_TEMPERATURE,
                                                        1 if sensor_available & 0b00000010 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SENSOR_AVAILABLE_TEMPERATURE_EU,
                                                        1 if sensor_available & 0b00000001 else 0, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BIN_1_DISTANCE,
                                                    bin_1_distance, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.TRANSMIT_PULSE_LENGTH,
                                                    transmit_pulse_length, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.REFERENCE_LAYER_START,
                                                    reference_layer_start, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.REFERENCE_LAYER_STOP,
                                                    reference_layer_stop, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.FALSE_TARGET_THRESHOLD,
                                                    false_target_threshold, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.TRANSMIT_LAG_DISTANCE,
                                                    transmit_lag_distance, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SERIAL_NUMBER,
                                                    serial_number, str))

        if self._file_type == AdcpFileType.ADCPS_File:
            # following parameters exist in ADCPS_JLN_INSTRUMENT particles
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSTEM_BANDWIDTH,
                                                        system_bandwidth, int))

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CPU_SERIAL_NUM,
                                                        cpu_serial_num, str))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SYSTEM_POWER,
                                                        system_power, int))

    def parse_variable_leader(self, data):
        """
        Parse the variable leader portion of the particle
        """
        rtc = {}
        rtc2 = {}

        (variable_leader_id, ensemble_number, rtc['year'], rtc['month'],
         rtc['day'], rtc['hour'], rtc['minute'], rtc['second'],
         rtc['hundredths'], ensemble_number_increment, error_bit_field,
         reserved_error_bit_field, speed_of_sound, transducer_depth, heading,
         pitch, roll, salinity, temperature, mpt_minutes, mpt_seconds_component,
         mpt_hundredths_component, heading_stdev, pitch_stdev, roll_stdev,
         adc_transmit_current, adc_transmit_voltage, adc_ambient_temp,
         adc_pressure_plus, adc_pressure_minus, adc_attitude_temp,
         adc_attitiude, adc_contamination_sensor, error_status_word_1,
         error_status_word_2, error_status_word_3, error_status_word_4) = struct.unpack_from('<2H10B3H2hHh18B', data)
        # Note: the ADCPS and ADCPA (glider) Variable Leader have extra bytes at end
        # This is unpacked lower in method

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ENSEMBLE_NUMBER,
                                                    ensemble_number, int))

        # convert individual date and time values to datetime object and
        # calculate the NTP timestamp (seconds since Jan 1, 1900), per OOI
        # convention
        dts = dt.datetime(2000 + rtc['year'], rtc['month'], rtc['day'],
                          rtc['hour'], rtc['minute'], rtc['second'])
        epoch_ts = timegm(dts.timetuple()) + (rtc['hundredths'] / 100.0)  # seconds since 1970-01-01 in UTC
        ntp_ts = ntplib.system_to_ntp_time(epoch_ts)

        self.set_internal_timestamp(ntp_ts)

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.REAL_TIME_CLOCK,
                                                    [rtc['year'], rtc['month'], rtc['day'],
                                                     rtc['hour'], rtc['minute'], rtc['second'],
                                                     rtc['hundredths']], list))
        # IDD calls for array of 8, may need to hard code century

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ENSEMBLE_START_TIME,
                                                    ntp_ts, float))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ENSEMBLE_NUMBER_INCREMENT,
                                                    ensemble_number_increment, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SPEED_OF_SOUND,
                                                    speed_of_sound, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.TRANSDUCER_DEPTH,
                                                    transducer_depth, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.HEADING,
                                                    heading, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PITCH,
                                                    pitch, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ROLL,
                                                    roll, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SALINITY,
                                                    salinity, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.TEMPERATURE,
                                                    temperature, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.MPT_MINUTES,
                                                    mpt_minutes, int))

        mpt_seconds = float(mpt_seconds_component + (mpt_hundredths_component / 100.0))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.MPT_SECONDS,
                                                    mpt_seconds, float))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.HEADING_STDEV,
                                                    heading_stdev, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PITCH_STDEV,
                                                    pitch_stdev, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ROLL_STDEV,
                                                    roll_stdev, int))

        # ADC_TRANSMIT_VOLTAGE is the only parameter in the ADC Channel group
        # that is used in all variants of PD0 based parsers & particles
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_TRANSMIT_VOLTAGE,
                                                    adc_transmit_voltage, int))

        # the remaining ADC Channels and the individual parts of the error status words
        # are only published for ADPCS/ADCPT file types, handled lower in method

        if self._file_type == AdcpFileType.ADCPS_File or self._file_type == AdcpFileType.ADCPA_FILE:
            # Only ADCPS and ADCPA (glider) contain the following bytes

            (SPARE1, pressure, pressure_variance, SPARE2) = \
                struct.unpack_from('<H3I', data[ADCPA_AUV_VARIABLE_LEADER_BYTES:ADCPA_VARIABLE_LEADER_BYTES])
            # bytes 47-60 (list indices 46-59)

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PRESSURE,
                                                        pressure, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PRESSURE_VARIANCE,
                                                        pressure_variance, int))

        if self._file_type == AdcpFileType.ADCPS_File or self._file_type == AdcpFileType.ADCPA_AUV_FILE:
            # ADCPS and ADCPA AUV decode all 8 ADC words, decode the BIT Result at the bit level
            # and have several Error Status Word fields in common

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_TRANSMIT_CURRENT,
                                                        adc_transmit_current, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_AMBIENT_TEMP,
                                                        adc_ambient_temp, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_PRESSURE_PLUS,
                                                        adc_pressure_plus, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_PRESSURE_MINUS,
                                                        adc_pressure_minus, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_ATTITUDE_TEMP,
                                                        adc_attitude_temp, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_ATTITUDE,
                                                        adc_attitiude, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADC_CONTAMINATION_SENSOR,
                                                        adc_contamination_sensor, int))

            # decode the BIT test byte
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BIT_RESULT_DEMOD_1,
                                                        1 if error_bit_field & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BIT_RESULT_DEMOD_0,
                                                        1 if error_bit_field & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BIT_RESULT_TIMING,
                                                        1 if error_bit_field & 0b00000010 else 0, int))

            # decode the error status word bytes
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BUS_ERROR_EXCEPTION,
                                                        1 if error_status_word_1 & 0b00000001 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ADDRESS_ERROR_EXCEPTION,
                                                        1 if error_status_word_1 & 0b00000010 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ILLEGAL_INSTRUCTION_EXCEPTION,
                                                        1 if error_status_word_1 & 0b00000100 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ZERO_DIVIDE_INSTRUCTION,
                                                        1 if error_status_word_1 & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.EMULATOR_EXCEPTION,
                                                        1 if error_status_word_1 & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.UNASSIGNED_EXCEPTION,
                                                        1 if error_status_word_1 & 0b00100000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PINGING,
                                                        1 if error_status_word_2 & 0b00000001 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.COLD_WAKEUP_OCCURRED,
                                                        1 if error_status_word_2 & 0b01000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.UNKNOWN_WAKEUP_OCCURRED,
                                                        1 if error_status_word_2 & 0b10000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CLOCK_READ_ERROR,
                                                        1 if error_status_word_3 & 0b00000001 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SPURIOUS_UART_INTERRUPT,
                                                        1 if error_status_word_4 & 0b00100000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SPURIOUS_CLOCK_INTERRUPT,
                                                        1 if error_status_word_4 & 0b01000000 else 0, int))

        else:
            # ADCPA simply puts out the BIT status word and the BIT error count
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BIT_ERROR_NUMBER,
                                                        error_bit_field, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BIT_ERROR_COUNT,
                                                        reserved_error_bit_field, int))

        if self._file_type == AdcpFileType.ADCPS_File:
            # ADCPS has some unique items from error status words and has the RTC2 bytes at the end
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATCHDOG_RESTART_OCCURRED,
                                                        1 if error_status_word_1 & 0b01000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BATTERY_SAVER_POWER,
                                                        1 if error_status_word_1 & 0b10000000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.UNEXPECTED_ALARM,
                                                        1 if error_status_word_3 & 0b00000010 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CLOCK_JUMP_FORWARD,
                                                        1 if error_status_word_3 & 0b00000100 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CLOCK_JUMP_BACKWARD,
                                                        1 if error_status_word_3 & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.POWER_FAIL,
                                                        1 if error_status_word_4 & 0b00001000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.SPURIOUS_DSP_INTERRUPT,
                                                        1 if error_status_word_4 & 0b00010000 else 0, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.LEVEL_7_INTERRUPT,
                                                        1 if error_status_word_4 & 0b10000000 else 0, int))

            # RTC 2 fields and corresponding outputs only exist for ADPCS/ADCPT file types
            # RTC2 values are last 8 bytes when provided
            (rtc2['century'], rtc2['year'], rtc2['month'],
             rtc2['day'], rtc2['hour'], rtc2['minute'], rtc2['second'],
             rtc2['hundredths']) = struct.unpack('<8B', data[-8:])

            dts = dt.datetime(rtc2['century'] * 100 + rtc2['year'], rtc2['month'], rtc2['day'],
                              rtc2['hour'], rtc2['minute'], rtc2['second'])

            epoch_ts = timegm(dts.timetuple()) + (rtc2['hundredths'] / 100.0)  # seconds since 1970-01-01 in UTC
            ntp_ts = ntplib.system_to_ntp_time(epoch_ts)

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.REAL_TIME_CLOCK2,
                                                        [rtc2['century'], rtc['year'], rtc['month'], rtc['day'],
                                                         rtc['hour'], rtc['minute'], rtc['second'],
                                                         rtc['hundredths']], list))

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ENSEMBLE_START_TIME2,
                                                        ntp_ts, float))
        elif self._file_type == AdcpFileType.ADCPA_AUV_FILE:
            # ADCPA AUV files get Power Fail error from a different bit in Error Status Word 4
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.POWER_FAIL,
                                                        1 if error_status_word_4 & 0b10000000 else 0, int))

    def parse_velocity_data(self, data):
        """
        Parse the velocity portion of the particle
        """

        num_cells = self.num_depth_cells
        offset = ID_BYTES

        water_velocity_1 = []
        water_velocity_2 = []
        water_velocity_3 = []
        error_velocity = []

        for row in range(0, num_cells):
            (a, b, c, d) = struct.unpack_from('<4h', data, offset)
            water_velocity_1.append(a)
            water_velocity_2.append(b)
            water_velocity_3.append(c)
            error_velocity.append(d)
            offset += VELOCITY_BYTES_PER_CELL

        if self._file_type == AdcpFileType.ADCPS_File or self._file_type == AdcpFileType.ADCPA_FILE:
            # ADCPS and ADCPA (glider) are configured in East, North, Up coordinates

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATER_VELOCITY_EAST,
                                                        water_velocity_1, list))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATER_VELOCITY_NORTH,
                                                        water_velocity_2, list))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATER_VELOCITY_UP,
                                                        water_velocity_3, list))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ERROR_VELOCITY,
                                                        error_velocity, list))
        else:  # ADCPA AUV use vehicle coordinates
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATER_VELOCITY_FORWARD,
                                                        water_velocity_1, list))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATER_VELOCITY_STARBOARD,
                                                        water_velocity_2, list))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.WATER_VELOCITY_VERTICAL,
                                                        water_velocity_3, list))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ERROR_VELOCITY,
                                                        error_velocity, list))

    def parse_correlation_magnitude_data(self, data):
        """
        Parse the correlation magnitude portion of the particle
        """
        num_cells = self.num_depth_cells
        offset = ID_BYTES

        correlation_magnitude_beam1 = []
        correlation_magnitude_beam2 = []
        correlation_magnitude_beam3 = []
        correlation_magnitude_beam4 = []
        for row in range(0, num_cells):
            (a, b, c, d) = struct.unpack_from('<4B', data, offset)
            correlation_magnitude_beam1.append(a)
            correlation_magnitude_beam2.append(b)
            correlation_magnitude_beam3.append(c)
            correlation_magnitude_beam4.append(d)
            offset += CORRELATION_BYTES_PER_CELL

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CORRELATION_MAGNITUDE_BEAM1,
                                                    correlation_magnitude_beam1, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CORRELATION_MAGNITUDE_BEAM2,
                                                    correlation_magnitude_beam2, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CORRELATION_MAGNITUDE_BEAM3,
                                                    correlation_magnitude_beam3, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.CORRELATION_MAGNITUDE_BEAM4,
                                                    correlation_magnitude_beam4, list))

    def parse_echo_intensity_data(self, data):
        """
        Parse the echo intensity portion of the particle
        """
        num_cells = self.num_depth_cells
        offset = ID_BYTES

        echo_intensity_beam1 = []
        echo_intensity_beam2 = []
        echo_intensity_beam3 = []
        echo_intensity_beam4 = []
        for row in range(0, num_cells):
            (a, b, c, d) = struct.unpack_from('<4B', data, offset)
            echo_intensity_beam1.append(a)
            echo_intensity_beam2.append(b)
            echo_intensity_beam3.append(c)
            echo_intensity_beam4.append(d)
            offset += ECHO_INTENSITY_BYTES_PER_CELL

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ECHO_INTENSITY_BEAM1,
                                                    echo_intensity_beam1, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ECHO_INTENSITY_BEAM2,
                                                    echo_intensity_beam2, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ECHO_INTENSITY_BEAM3,
                                                    echo_intensity_beam3, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.ECHO_INTENSITY_BEAM4,
                                                    echo_intensity_beam4, list))

    def parse_percent_good_data(self, data):
        """
        Parse the percent good portion of the particle

        """
        num_cells = self.num_depth_cells
        offset = ID_BYTES

        percent_good_3beam = []
        percent_transforms_reject = []
        percent_bad_beams = []
        percent_good_4beam = []
        for row in range(0, num_cells):
            (a, b, c, d) = struct.unpack_from('<4B', data, offset)
            percent_good_3beam.append(a)
            percent_transforms_reject.append(b)
            percent_bad_beams.append(c)
            percent_good_4beam.append(d)
            offset += PERCENT_GOOD_BYTES_PER_CELL

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PERCENT_GOOD_3BEAM,
                                                    percent_good_3beam, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PERCENT_TRANSFORMS_REJECT,
                                                    percent_transforms_reject, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PERCENT_BAD_BEAMS,
                                                    percent_bad_beams, list))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.PERCENT_GOOD_4BEAM,
                                                    percent_good_4beam, list))

    def parse_bottom_track_data(self, data):
        """
        Parse the bottom track portion of the particle

        """

        # info statement to find a record with bottom track data!

        (bottom_track_id, bt_pings_per_ensemble, bt_delay_before_reacquire,
         bt_corr_magnitude_min, bt_amp_magnitude_min, bt_percent_good_min,
         bt_mode, bt_error_velocity_max, RESERVED, beam1_bt_range_lsb, beam2_bt_range_lsb,
         beam3_bt_range_lsb, beam4_bt_range_lsb, beam1_bt_velocity,
         beam2_bt_velocity, beam3_bt_velocity, beam4_bt_velocity,
         beam1_bt_correlation, beam2_bt_correlation, beam3_bt_correlation,
         beam4_bt_correlation, beam1_eval_amp, beam2_eval_amp, beam3_eval_amp,
         beam4_eval_amp, beam1_bt_percent_good, beam2_bt_percent_good,
         beam3_bt_percent_good, beam4_bt_percent_good, ref_layer_min,
         ref_layer_near, ref_layer_far, beam1_ref_layer_velocity,
         beam2_ref_layer_velocity, beam3_ref_layer_velocity,
         beam4_ref_layer_velocity, beam1_ref_correlation, beam2_ref_correlation,
         beam3_ref_correlation, beam4_ref_correlation, beam1_ref_intensity,
         beam2_ref_intensity, beam3_ref_intensity, beam4_ref_intensity,
         beam1_ref_percent_good, beam2_ref_percent_good, beam3_ref_percent_good,
         beam4_ref_percent_good, bt_max_depth, beam1_rssi_amplitude,
         beam2_rssi_amplitude, beam3_rssi_amplitude, beam4_rssi_amplitude,
         bt_gain, beam1_bt_range_msb, beam2_bt_range_msb, beam3_bt_range_msb,
         beam4_bt_range_msb) = \
            struct.unpack_from('<3H4BHL4H4h12B3H4h12BH9B', data)
        # Note, ADCPS has 4 additional reserved bytes at the end, which are
        # not needed for either particle

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_PINGS_PER_ENSEMBLE,
                                                    bt_pings_per_ensemble, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_DELAY_BEFORE_REACQUIRE,
                                                    bt_delay_before_reacquire, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_CORR_MAGNITUDE_MIN,
                                                    bt_corr_magnitude_min, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_EVAL_MAGNITUDE_MIN,
                                                    bt_amp_magnitude_min, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_PERCENT_GOOD_MIN,
                                                    bt_percent_good_min, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_MODE,
                                                    bt_mode, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_ERROR_VELOCITY_MAX,
                                                    bt_error_velocity_max, int))

        # need to combine LSBs and MSBs of ranges
        beam1_bt_range = beam1_bt_range_lsb + (beam1_bt_range_msb << 16)
        beam2_bt_range = beam2_bt_range_lsb + (beam2_bt_range_msb << 16)
        beam3_bt_range = beam3_bt_range_lsb + (beam3_bt_range_msb << 16)
        beam4_bt_range = beam4_bt_range_lsb + (beam4_bt_range_msb << 16)

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_RANGE,
                                                    beam1_bt_range, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_RANGE,
                                                    beam2_bt_range, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_RANGE,
                                                    beam3_bt_range, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_RANGE,
                                                    beam4_bt_range, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_CORRELATION,
                                                    beam1_bt_correlation, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_CORRELATION,
                                                    beam2_bt_correlation, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_CORRELATION,
                                                    beam3_bt_correlation, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_CORRELATION,
                                                    beam4_bt_correlation, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_EVAL_AMP,
                                                    beam1_eval_amp, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_EVAL_AMP,
                                                    beam2_eval_amp, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_EVAL_AMP,
                                                    beam3_eval_amp, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_EVAL_AMP,
                                                    beam4_eval_amp, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_PERCENT_GOOD,
                                                    beam1_bt_percent_good, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_PERCENT_GOOD,
                                                    beam2_bt_percent_good, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_PERCENT_GOOD,
                                                    beam3_bt_percent_good, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_PERCENT_GOOD,
                                                    beam4_bt_percent_good, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_REF_LAYER_MIN,
                                                    ref_layer_min, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_REF_LAYER_NEAR,
                                                    ref_layer_near, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_REF_LAYER_FAR,
                                                    ref_layer_far, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_REF_CORRELATION,
                                                    beam1_ref_correlation, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_REF_CORRELATION,
                                                    beam2_ref_correlation, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_REF_CORRELATION,
                                                    beam3_ref_correlation, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_REF_CORRELATION,
                                                    beam4_ref_correlation, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_REF_INTENSITY,
                                                    beam1_ref_intensity, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_REF_INTENSITY,
                                                    beam2_ref_intensity, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_REF_INTENSITY,
                                                    beam3_ref_intensity, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_REF_INTENSITY,
                                                    beam4_ref_intensity, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_REF_PERCENT_GOOD,
                                                    beam1_ref_percent_good, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_REF_PERCENT_GOOD,
                                                    beam2_ref_percent_good, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_REF_PERCENT_GOOD,
                                                    beam3_ref_percent_good, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_REF_PERCENT_GOOD,
                                                    beam4_ref_percent_good, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_MAX_DEPTH,
                                                    bt_max_depth, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM1_RSSI_AMPLITUDE,
                                                    beam1_rssi_amplitude, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM2_RSSI_AMPLITUDE,
                                                    beam2_rssi_amplitude, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM3_RSSI_AMPLITUDE,
                                                    beam3_rssi_amplitude, int))
        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_BEAM4_RSSI_AMPLITUDE,
                                                    beam4_rssi_amplitude, int))

        self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_GAIN,
                                                    bt_gain, int))

        if self._file_type == AdcpFileType.ADCPS_File or self._file_type == AdcpFileType.ADCPA_FILE:
            # ADCPS and ADCPA (glider) are configured in East, North, Up coordinates
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_EASTWARD_VELOCITY,
                                                        beam1_bt_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_NORTHWARD_VELOCITY,
                                                        beam2_bt_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_UPWARD_VELOCITY,
                                                        beam3_bt_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_ERROR_VELOCITY,
                                                        beam4_bt_velocity, int))

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_EASTWARD_REF_LAYER_VELOCITY,
                                                        beam1_ref_layer_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_NORTHWARD_REF_LAYER_VELOCITY,
                                                        beam2_ref_layer_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_UPWARD_REF_LAYER_VELOCITY,
                                                        beam3_ref_layer_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_ERROR_REF_LAYER_VELOCITY,
                                                        beam4_ref_layer_velocity, int))
        else:  # ADCPA AUV use vehicle coordinates
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_FORWARD_VELOCITY,
                                                        beam1_bt_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_STARBOARD_VELOCITY,
                                                        beam2_bt_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_VERTICAL_VELOCITY,
                                                        beam3_bt_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_ERROR_VELOCITY,
                                                        beam4_bt_velocity, int))

            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_FORWARD_REF_LAYER_VELOCITY,
                                                        beam1_ref_layer_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_STARBOARD_REF_LAYER_VELOCITY,
                                                        beam2_ref_layer_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_VERTICAL_REF_LAYER_VELOCITY,
                                                        beam3_ref_layer_velocity, int))
            self.final_result.append(self._encode_value(AdcpPd0ParserDataParticleKey.BT_ERROR_REF_LAYER_VELOCITY,
                                                        beam4_ref_layer_velocity, int))


class AdcpPd0Parser(SimpleParser):

    def parse_file(self):
        """
        Entry point into parsing the file
        Loop through the file one ensemble at a time
        """

        position = 0  # set position to beginning of file
        header_id_bytes = self._stream_handle.read(2)  # read the first two bytes of the file

        while header_id_bytes:  # will be None when EOF is found

            if header_id_bytes == ADCPS_PD0_HEADER_REGEX:

                # get the ensemble size from the next 2 bytes (excludes checksum bytes)
                num_bytes = struct.unpack("<H", self._stream_handle.read(2))[0]

                self._stream_handle.seek(position)  # reset to beginning of ensemble
                input_buffer = self._stream_handle.read(num_bytes + 2)  # read entire ensemble

                if len(input_buffer) == num_bytes + 2:  # make sure there are enough bytes including checksum

                    checksum = sum([ord(x) for x in input_buffer[:num_bytes]]) & CHECKSUM_MODULO
                    message_checksum = struct.unpack("<H", input_buffer[-2:])[0]

                    if checksum == message_checksum:

                        particle = self._extract_sample(self._particle_class, None, input_buffer, None)
                        self._record_buffer.append(particle)

                    else:  # checksum failure
                        log.warn('checksum did NOT match')
                        self._exception_callback(RecoverableSampleException("Ensemble checksum failed"))

                else:  # reached EOF
                    log.warn("not enough bytes left for complete ensemble")
                    self._exception_callback(UnexpectedDataException("Found incomplete ensemble at end of file"))

            else:  # did not get header ID bytes
                log.warn('did not fine header ID bytes')
                self._exception_callback(RecoverableSampleException(
                    "Did not find Header ID bytes where expected, trying next 2 bytes"))

            position = self._stream_handle.tell()  # set the new file position
            header_id_bytes = self._stream_handle.read(2)  # read the next two bytes of the file
