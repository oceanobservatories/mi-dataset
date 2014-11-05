#!/usr/bin/env python

"""
@package mi.dataset.parser
@file /mi/dataset/parser/velpt_ab_dcl.py
@author Chris Goodrich
@brief Parser for the velpt_ab_dcl recovered and telemetered dataset driver
Release notes:

initial release
"""
__author__ = 'Chris Goodrich'
__license__ = 'Apache 2.0'

import struct
import calendar
from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, DataParticleKey


class VelptDataParticleType(BaseEnum):
    VELPT_AB_DCL_INSTRUMENT = 'velpt_ab_dcl_instrument'
    VELPT_AB_DCL_DIAGNOSTICS_METADATA = 'velpt_ab_dcl_diagnostics_metadata'
    VELPT_AB_DCL_DIAGNOSTICS = 'velpt_ab_dcl_diagnostics'
    VELPT_AB_DCL_INSTRUMENT_RECOVERED = 'velpt_ab_dcl_instrument_recovered'
    VELPT_AB_DCL_DIAGNOSTICS_METADATA_RECOVERED = 'velpt_ab_dcl_diagnostics_metadata_recovered'
    VELPT_AB_DCL_DIAGNOSTICS_RECOVERED = 'velpt_ab_dcl_diagnostics_recovered'


class VelptAbDataParticleKey(BaseEnum):
    DATE_TIME_STRING = 'date_time_string'                      # PD93
    ERROR_CODE = 'error_code'                                  # PD433
    ANALOG1 = 'analog1'                                        # PD434
    BATTERY_VOLTAGE = 'battery_voltage'                        # PD432
    SOUND_SPEED_ANALOG2 = 'sound_speed_analog2'                # PD435
    HEADING = 'heading'                                        # PD436
    PITCH = 'pitch'                                            # PD437
    ROLL = 'roll'                                              # PD438
    PRESSURE = 'pressure'                                      # PD2
    STATUS = 'status'                                          # PD439
    TEMPERATURE = 'temperature'                                # PD440
    VELOCITY_BEAM1 = 'velocity_beam1'                          # PD441
    VELOCITY_BEAM2 = 'velocity_beam2'                          # PD442
    VELOCITY_BEAM3 = 'velocity_beam3'                          # PD443
    AMPLITUDE_BEAM1 = 'amplitude_beam1'                        # PD444
    AMPLITUDE_BEAM2 = 'amplitude_beam2'                        # PD445
    AMPLITUDE_BEAM3 = 'amplitude_beam3'                        # PD446
    RECORDS_TO_FOLLOW = 'records_to_follow'                    # PD447
    CELL_NUMBER_DIAGNOSTICS = 'cell_number_diagnostics'        # PD448
    NOISE_AMPLITUDE_BEAM1 = 'noise_amplitude_beam1'            # PD449
    NOISE_AMPLITUDE_BEAM2 = 'noise_amplitude_beam2'            # PD450
    NOISE_AMPLITUDE_BEAM3 = 'noise_amplitude_beam3'            # PD451
    NOISE_AMPLITUDE_BEAM4 = 'noise_amplitude_beam4'            # PD452
    PROCESSING_MAGNITUDE_BEAM1 = 'processing_magnitude_beam1'  # PD453
    PROCESSING_MAGNITUDE_BEAM2 = 'processing_magnitude_beam2'  # PD454
    PROCESSING_MAGNITUDE_BEAM3 = 'processing_magnitude_beam3'  # PD455
    PROCESSING_MAGNITUDE_BEAM4 = 'processing_magnitude_beam4'  # PD456
    DISTANCE_BEAM1 = 'distance_beam1'                          # PD457
    DISTANCE_BEAM2 = 'distance_beam2'                          # PD458
    DISTANCE_BEAM3 = 'distance_beam3'                          # PD459
    DISTANCE_BEAM4 = 'distance_beam4'                          # PD460


class VelptAbDataParticle(DataParticle):
    """
    Class for creating the metadata & data particles for velpt_ab_dcl
    """
    # Offsets for date-time group in velocity and diagnostics data records
    minute_offset = 4
    second_offset = 5
    day_offset = 6
    hour_offset = 7
    year_offset = 8
    month_offset = 9

    # Offsets for data needed to build particles
    error_code_offset = 10
    analog1_offset = 12
    battery_voltage_offset = 14
    sound_speed_analog2_offset = 16
    heading_offset = 18
    pitch_offset = 20
    roll_offset = 22
    pressure_msb_offset = 24
    status_offset = 25
    pressure_lsw_offset = 26
    temperature_offset = 28
    velocity_beam1_offset = 30
    velocity_beam2_offset = 32
    velocity_beam3_offset = 34
    amplitude_beam1_offset = 36
    amplitude_beam2_offset = 37
    amplitude_beam3_offset = 38

    # Offsets for diagnostics header records
    records_to_follow_offset = 4
    cell_number_diagnostics_offset = 6
    noise_amplitude_beam1_offset = 8
    noise_amplitude_beam2_offset = 9
    noise_amplitude_beam3_offset = 10
    noise_amplitude_beam4_offset = 11
    processing_magnitude_beam1_offset = 12
    processing_magnitude_beam2_offset = 14
    processing_magnitude_beam3_offset = 16
    processing_magnitude_beam4_offset = 18
    distance_beam1_offset = 20
    distance_beam2_offset = 22
    distance_beam3_offset = 24
    distance_beam4_offset = 26

    @staticmethod
    def _convert_bcd_to_decimal(in_val):
        """
        Converts Binary Coded Decimal to a decimal value
        :param in_val: The value to convert
        :return: The decimal value
        """
        tens = (struct.unpack('B', in_val)[0]) >> 4
        actual = struct.unpack('B', in_val)[0]
        low_byte = tens << 4
        return (tens*10) + (actual-low_byte)

    @staticmethod
    def _convert_bcd_to_string(in_val):
        """
        Converts Binary Coded Decimal to a string
        :param in_val: The value to convert
        :return: The string value
        """
        tens = (struct.unpack('B', in_val)[0]) >> 4
        part1 = struct.pack('B', tens+48)
        actual = struct.unpack('B', in_val)[0]
        low_byte = tens << 4
        part2 = struct.pack('B', (actual-low_byte)+48)
        return part1 + part2

    @staticmethod
    def get_date_time_string(record):
        """
        Convert the date and time from the record to the standard string YYYY/MM/DD HH:MM:SS
        :param record: The record read from the file which contains the date and time
        :return: The date time string
        """
        year = '20' + VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.year_offset])
        month = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.month_offset])
        day = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.day_offset])
        hour = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.hour_offset])
        minute = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.minute_offset])
        second = VelptAbDataParticle._convert_bcd_to_string(record[VelptAbDataParticle.second_offset])
        return year+'/'+month+'/'+day+' '+hour+':'+minute+':'+second

    @staticmethod
    def get_timestamp(record):
        """
        Convert the date and time from the record to a Unix timestamp
        :param record: The record read from the file which contains the date and time
        :return: the Unix timestamp
        """
        year = 2000 + VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.year_offset])
        month = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.month_offset])
        day = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.day_offset])
        hour = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.hour_offset])
        minute = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.minute_offset])
        second = VelptAbDataParticle._convert_bcd_to_decimal(record[VelptAbDataParticle.second_offset])
        timestamp = (year, month, day, hour, minute, second, 0, 0, 0)
        elapsed_seconds = calendar.timegm(timestamp)
        return elapsed_seconds

    @staticmethod
    def get_diagnostics_count(record):
        """
        Read the expected number of diagnostics records to follow the header
        :param record: The record read from the file which contains the date and time
        :return: The number of expected diagnostics records.
        """
        return struct.unpack('<h', record[VelptAbDataParticle.records_to_follow_offset:
                                          VelptAbDataParticle.cell_number_diagnostics_offset])[0]

    @staticmethod
    def generate_data_dict(self, record):
        """
        Pull the needed fields from the data file and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param record: The record read from the file which contains the date and time
        :return: The dictionary
        """

        date_time_string = VelptAbDataParticle.get_date_time_string(record)

        error_code = struct.unpack('<h', record[VelptAbDataParticle.error_code_offset:
                                                VelptAbDataParticle.analog1_offset])[0]
        analog_1 = struct.unpack('<h', record[VelptAbDataParticle.analog1_offset:
                                              VelptAbDataParticle.battery_voltage_offset])[0]
        battery_voltage = struct.unpack('<h', record[VelptAbDataParticle.battery_voltage_offset:
                                                     VelptAbDataParticle.sound_speed_analog2_offset])[0] * 0.1
        sound_speed_analog_2 = struct.unpack('<h', record[VelptAbDataParticle.sound_speed_analog2_offset:
                                                          VelptAbDataParticle.heading_offset])[0] * 0.1
        heading = struct.unpack('<h', record[VelptAbDataParticle.heading_offset:
                                             VelptAbDataParticle.pitch_offset])[0] * 0.1
        pitch = struct.unpack('<h', record[VelptAbDataParticle.pitch_offset:
                                           VelptAbDataParticle.roll_offset])[0] * 0.1
        roll = struct.unpack('<h', record[VelptAbDataParticle.roll_offset:
                                          VelptAbDataParticle.pressure_msb_offset])[0] * 0.1
        pressure = (struct.unpack('B', record[VelptAbDataParticle.pressure_msb_offset:
                                              VelptAbDataParticle.status_offset])[0] * 65536.0) +\
                   (struct.unpack('<h', record[VelptAbDataParticle.pressure_lsw_offset:
                                               VelptAbDataParticle.temperature_offset])[0] * 0.001)
        status = struct.unpack('B', record[VelptAbDataParticle.status_offset:
                                           VelptAbDataParticle.pressure_lsw_offset])[0]
        temperature = struct.unpack('<h', record[VelptAbDataParticle.temperature_offset:
                                                 VelptAbDataParticle.velocity_beam1_offset])[0] * 0.01
        velocity_beam_1 = struct.unpack('<h', record[VelptAbDataParticle.velocity_beam1_offset:
                                                     VelptAbDataParticle.velocity_beam2_offset])[0] * 1.0
        velocity_beam_2 = struct.unpack('<h', record[VelptAbDataParticle.velocity_beam2_offset:
                                                     VelptAbDataParticle.velocity_beam3_offset])[0] * 1.0
        velocity_beam_3 = struct.unpack('<h', record[VelptAbDataParticle.velocity_beam3_offset:
                                                     VelptAbDataParticle.amplitude_beam1_offset])[0] * 1.0
        amplitude_beam_1 = struct.unpack('B', record[VelptAbDataParticle.amplitude_beam1_offset:
                                                     VelptAbDataParticle.amplitude_beam2_offset])[0]
        amplitude_beam_2 = struct.unpack('B', record[VelptAbDataParticle.amplitude_beam2_offset:
                                                     VelptAbDataParticle.amplitude_beam3_offset])[0]
        amplitude_beam_3 = struct.unpack('B', record[VelptAbDataParticle.amplitude_beam3_offset:
                                                     VelptAbDataParticle.amplitude_beam3_offset+1])[0]

        return {VelptAbDataParticleKey.DATE_TIME_STRING: date_time_string,
                VelptAbDataParticleKey.ERROR_CODE: error_code,
                VelptAbDataParticleKey.ANALOG1: analog_1,
                VelptAbDataParticleKey.BATTERY_VOLTAGE: battery_voltage,
                VelptAbDataParticleKey.SOUND_SPEED_ANALOG2: sound_speed_analog_2,
                VelptAbDataParticleKey.HEADING: heading,
                VelptAbDataParticleKey.PITCH: pitch,
                VelptAbDataParticleKey.ROLL: roll,
                VelptAbDataParticleKey.PRESSURE: pressure,
                VelptAbDataParticleKey.STATUS: status,
                VelptAbDataParticleKey.TEMPERATURE: temperature,
                VelptAbDataParticleKey.VELOCITY_BEAM1: velocity_beam_1,
                VelptAbDataParticleKey.VELOCITY_BEAM2: velocity_beam_2,
                VelptAbDataParticleKey.VELOCITY_BEAM3: velocity_beam_3,
                VelptAbDataParticleKey.AMPLITUDE_BEAM1: amplitude_beam_1,
                VelptAbDataParticleKey.AMPLITUDE_BEAM2: amplitude_beam_2,
                VelptAbDataParticleKey.AMPLITUDE_BEAM3: amplitude_beam_3}

    @staticmethod
    def generate_diagnostics_header_dict(self, date_time_string, record):
        """
        Pull the needed fields from the data file and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param record: The record read from the file which contains the date and time
        :return: The dictionary
        """
        records_to_follow = struct.unpack('<h', record[VelptAbDataParticle.records_to_follow_offset:
                                                       VelptAbDataParticle.cell_number_diagnostics_offset])[0]
        cell_number_diagnostics = struct.unpack('<h', record[VelptAbDataParticle.cell_number_diagnostics_offset:
                                                             VelptAbDataParticle.noise_amplitude_beam1_offset])[0]
        noise_amplitude_beam1 = struct.unpack('B', record[VelptAbDataParticle.noise_amplitude_beam1_offset:
                                                          VelptAbDataParticle.noise_amplitude_beam2_offset])[0]
        noise_amplitude_beam2 = struct.unpack('B', record[VelptAbDataParticle.noise_amplitude_beam2_offset:
                                                          VelptAbDataParticle.noise_amplitude_beam3_offset])[0]
        noise_amplitude_beam3 = struct.unpack('B', record[VelptAbDataParticle.noise_amplitude_beam3_offset:
                                                          VelptAbDataParticle.noise_amplitude_beam4_offset])[0]
        noise_amplitude_beam4 = struct.unpack('B', record[VelptAbDataParticle.noise_amplitude_beam4_offset:
                                                          VelptAbDataParticle.processing_magnitude_beam1_offset])[0]
        processing_magnitude_beam1 = struct.unpack('<h', record[VelptAbDataParticle.processing_magnitude_beam1_offset:
                                                                VelptAbDataParticle.
                                                   processing_magnitude_beam2_offset])[0]
        processing_magnitude_beam2 = struct.unpack('<h', record[VelptAbDataParticle.processing_magnitude_beam2_offset:
                                                                VelptAbDataParticle.
                                                   processing_magnitude_beam3_offset])[0]
        processing_magnitude_beam3 = struct.unpack('<h', record[VelptAbDataParticle.processing_magnitude_beam3_offset:
                                                                VelptAbDataParticle.
                                                   processing_magnitude_beam4_offset])[0]
        processing_magnitude_beam4 = struct.unpack('<h', record[VelptAbDataParticle.processing_magnitude_beam4_offset:
                                                                VelptAbDataParticle.distance_beam1_offset])[0]
        distance_beam1 = struct.unpack('<h', record[VelptAbDataParticle.distance_beam1_offset:
                                                    VelptAbDataParticle.distance_beam2_offset])[0]
        distance_beam2 = struct.unpack('<h', record[VelptAbDataParticle.distance_beam2_offset:
                                                    VelptAbDataParticle.distance_beam3_offset])[0]
        distance_beam3 = struct.unpack('<h', record[VelptAbDataParticle.distance_beam3_offset:
                                                    VelptAbDataParticle.distance_beam4_offset])[0]
        distance_beam4 = struct.unpack('<h', record[VelptAbDataParticle.distance_beam4_offset:
                                                    VelptAbDataParticle.distance_beam4_offset+2])[0]

        return {VelptAbDataParticleKey.DATE_TIME_STRING: date_time_string,
                VelptAbDataParticleKey.RECORDS_TO_FOLLOW: records_to_follow,
                VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS: cell_number_diagnostics,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1: noise_amplitude_beam1,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2: noise_amplitude_beam2,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3: noise_amplitude_beam3,
                VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4: noise_amplitude_beam4,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1: processing_magnitude_beam1,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2: processing_magnitude_beam2,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3: processing_magnitude_beam3,
                VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4: processing_magnitude_beam4,
                VelptAbDataParticleKey.DISTANCE_BEAM1: distance_beam1,
                VelptAbDataParticleKey.DISTANCE_BEAM2: distance_beam2,
                VelptAbDataParticleKey.DISTANCE_BEAM3: distance_beam3,
                VelptAbDataParticleKey.DISTANCE_BEAM4: distance_beam4}


class VelptAbInstrumentDataParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptDataParticleType.VELPT_AB_DCL_INSTRUMENT

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BATTERY_VOLTAGE,
                                                      self.raw_data[VelptAbDataParticleKey.BATTERY_VOLTAGE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_ANALOG2,
                                                      self.raw_data[VelptAbDataParticleKey.SOUND_SPEED_ANALOG2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEADING,
                                                      self.raw_data[VelptAbDataParticleKey.HEADING], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PITCH,
                                                      self.raw_data[VelptAbDataParticleKey.PITCH], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ROLL,
                                                      self.raw_data[VelptAbDataParticleKey.ROLL], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE,
                                                      self.raw_data[VelptAbDataParticleKey.PRESSURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TEMPERATURE,
                                                      self.raw_data[VelptAbDataParticleKey.TEMPERATURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM1], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM3], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbDiagnosticsHeaderParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptDataParticleType.VELPT_AB_DCL_DIAGNOSTICS_METADATA

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.RECORDS_TO_FOLLOW,
                                                      self.raw_data[VelptAbDataParticleKey.RECORDS_TO_FOLLOW], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS,
                                                      self.raw_data[VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM4], int))

        return particle_parameters


class VelptAbDiagnosticsDataParticle(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptDataParticleType.VELPT_AB_DCL_DIAGNOSTICS

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BATTERY_VOLTAGE,
                                                      self.raw_data[VelptAbDataParticleKey.BATTERY_VOLTAGE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_ANALOG2,
                                                      self.raw_data[VelptAbDataParticleKey.SOUND_SPEED_ANALOG2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEADING,
                                                      self.raw_data[VelptAbDataParticleKey.HEADING], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PITCH,
                                                      self.raw_data[VelptAbDataParticleKey.PITCH], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ROLL,
                                                      self.raw_data[VelptAbDataParticleKey.ROLL], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE,
                                                      self.raw_data[VelptAbDataParticleKey.PRESSURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TEMPERATURE,
                                                      self.raw_data[VelptAbDataParticleKey.TEMPERATURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM1], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM3], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbInstrumentDataParticleRecovered(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptDataParticleType.VELPT_AB_DCL_INSTRUMENT_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BATTERY_VOLTAGE,
                                                      self.raw_data[VelptAbDataParticleKey.BATTERY_VOLTAGE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_ANALOG2,
                                                      self.raw_data[VelptAbDataParticleKey.SOUND_SPEED_ANALOG2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEADING,
                                                      self.raw_data[VelptAbDataParticleKey.HEADING], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PITCH,
                                                      self.raw_data[VelptAbDataParticleKey.PITCH], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ROLL,
                                                      self.raw_data[VelptAbDataParticleKey.ROLL], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE,
                                                      self.raw_data[VelptAbDataParticleKey.PRESSURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TEMPERATURE,
                                                      self.raw_data[VelptAbDataParticleKey.TEMPERATURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM1], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM3], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters


class VelptAbDiagnosticsHeaderParticleRecovered(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptDataParticleType.VELPT_AB_DCL_DIAGNOSTICS_METADATA_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.RECORDS_TO_FOLLOW,
                                                      self.raw_data[VelptAbDataParticleKey.RECORDS_TO_FOLLOW], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS,
                                                      self.raw_data[VelptAbDataParticleKey.CELL_NUMBER_DIAGNOSTICS],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.NOISE_AMPLITUDE_BEAM4], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM1],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM2],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM3],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.PROCESSING_MAGNITUDE_BEAM4],
                                                      int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM3], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DISTANCE_BEAM4,
                                                      self.raw_data[VelptAbDataParticleKey.DISTANCE_BEAM4], int))

        return particle_parameters


class VelptAbDiagnosticsDataParticleRecovered(VelptAbDataParticle):
    """
    See the IDD
    """
    _data_particle_type = VelptDataParticleType.VELPT_AB_DCL_DIAGNOSTICS_RECOVERED

    def _build_parsed_values(self):

        particle_parameters = []

        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.DATE_TIME_STRING,
                                                      self.raw_data[VelptAbDataParticleKey.DATE_TIME_STRING], str))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ERROR_CODE,
                                                      self.raw_data[VelptAbDataParticleKey.ERROR_CODE], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ANALOG1,
                                                      self.raw_data[VelptAbDataParticleKey.ANALOG1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.BATTERY_VOLTAGE,
                                                      self.raw_data[VelptAbDataParticleKey.BATTERY_VOLTAGE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.SOUND_SPEED_ANALOG2,
                                                      self.raw_data[VelptAbDataParticleKey.SOUND_SPEED_ANALOG2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.HEADING,
                                                      self.raw_data[VelptAbDataParticleKey.HEADING], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PITCH,
                                                      self.raw_data[VelptAbDataParticleKey.PITCH], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.ROLL,
                                                      self.raw_data[VelptAbDataParticleKey.ROLL], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.PRESSURE,
                                                      self.raw_data[VelptAbDataParticleKey.PRESSURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.STATUS,
                                                      self.raw_data[VelptAbDataParticleKey.STATUS], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.TEMPERATURE,
                                                      self.raw_data[VelptAbDataParticleKey.TEMPERATURE], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM1], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM2], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.VELOCITY_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.VELOCITY_BEAM3], float))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM1,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM1], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM2,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM2], int))
        particle_parameters.append(self._encode_value(VelptAbDataParticleKey.AMPLITUDE_BEAM3,
                                                      self.raw_data[VelptAbDataParticleKey.AMPLITUDE_BEAM3], int))

        return particle_parameters