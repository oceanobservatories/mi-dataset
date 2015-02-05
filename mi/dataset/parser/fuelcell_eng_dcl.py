#!/usr/bin/env python

"""
@package mi.dataset.parser.fuelcell_eng_dcl
@file mi/dataset/parser/fuelcell_eng_dcl.py
@author Chris Goodrich
@brief Parser for the fuelcell_eng_dcl dataset driver
Release notes:

initial release
"""
__author__ = 'cgoodrich'
__license__ = 'Apache 2.0'

import re
import calendar
import ntplib

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import RecoverableSampleException, ConfigurationException
from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.core.instrument.data_particle import DataParticle
from mi.dataset.parser.common_regexes import SPACE_REGEX, DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX
"""
Composition of a properly formed line of data:

    The DCL timestamp:
    -----------------------
    YYYY/MM/DD HH:MM:SS.SSS

    We don't care about the data between the timestamp and the start of the fuel cell data,
    which looks like this (all values are delimited by spaces):
    -------------------------------------------------------------------------------------------
    PwrSys psc: 25.30 4266.67 89.0 01c0 00000000 00000000
    pv1 1 25.37 0.00 pv2 1 25.27 0.00 pv3 1 25.27 0.00 pv4 1 25.20 2.00
    wt1 1 25.30 0.00 wt2 1 25.23 0.00
    fc1 1 0.00 0.00 fc2 1 0.00 0.00
    bt1 8.52 25.26 -742.00 bt2 8.71 25.23 -779.00 bt3 8.71 25.24 -767.00 bt4 8.71 25.24 -799.00
    ext 25.30 2.00
    int 25.33 55.50 25.30
    fcl 0.00
    swg 0 1.00 2.00
    cvt 1 376.78 138.00 1 18.35 00000000

    This is the fuel cell data we are interested in (all values are separated by commas:
    ---------------------------------------------------------------------------------------------
    4112,33557475,4308795,31356,13465,4260,10819,589,162678,46,21,100,15778,4,906397,-147897,661,
    -142057,660,85540,643,569,479,67108864,101728580,8472576,2097216:8002

    The value after the colon in the line above is the checksum for just the fuel cell data.

    The last entry on each line is typically a four-digit hexadecimal number, which is not used.
    ----
    6d47
"""

# Regular expressions (regex) used to parse a line of data
COMMA = ','

DATE_TIME_REGEX = r'(' + DATE_YYYY_MM_DD_REGEX + SPACE_REGEX + TIME_HR_MIN_SEC_MSEC_REGEX + r')'

DATE_MATCHER = re.compile(DATE_TIME_REGEX)

NON_DATA_REGEX = r'.+ No_FC_Data'
NON_DATA_MATCHER = re.compile(NON_DATA_REGEX)

START_DATA_REGEX = r'( [+-]?[0-9]+,)'
START_DATA_MATCHER = re.compile(START_DATA_REGEX)
END_DATA_REGEX = r'(: ?[+-]?[0-9]+ [0-9A-Fa-f]+)'
END_DATA_MATCHER = re.compile(END_DATA_REGEX)
COLON_REGEX = r'(:)'
COLON_MATCHER = re.compile(COLON_REGEX)

# Regex group indices
YEAR_GROUP = 2
MONTH_GROUP = 3
DAY_GROUP = 4
HOUR_GROUP = 5
MINUTE_GROUP = 6
SECONDS_GROUP = 7
MILLISECONDS_GROUP = 8

# Indices used to access the data parsed from a line of data
DATALOG_MANAGER_VERSION_INDEX = 0
SYSTEM_SOFTWARE_VERSION_INDEX = 1
TOTAL_RUN_TIME_INDEX = 2
FUEL_CELL_VOLTAGE_INDEX = 3
FUEL_CELL_CURRENT_INDEX = 4
REFORMER_TEMPERATURE_INDEX = 5
FUEL_CELL_H2_PRESSURE_INDEX = 6
FUEL_CELL_TEMPERATURE_INDEX = 7
REFORMER_FUEL_PRESSURE_INDEX = 8
FUEL_PUMP_PWM_DRIVE_PERCENT_INDEX = 9
AIR_PUMP_PWM_DRIVE_PERCENT_INDEX = 10
COOLANT_PUMP_PWM_DRIVE_PERCENT_INDEX = 11
AIR_PUMP_TACH_COUNT_INDEX = 12
FUEL_CELL_STATE_INDEX = 13
FUEL_REMAINING_INDEX = 14
POWER_TO_BATTERY1_INDEX = 15
BATTERY1_CONVERTER_TEMPERATURE_INDEX = 16
POWER_TO_BATTERY2_INDEX = 17
BATTERY2_CONVERTER_TEMPERATURE_INDEX = 18
BALANCE_OF_PLANT_POWER_INDEX = 19
BALANCE_OF_PLANT_CONVERTER_TEMPERATURE_INDEX = 20
POWER_BOARD_TEMPERATURE_INDEX = 21
CONTROL_BOARD_TEMPERATURE_INDEX = 22
POWER_MANAGER_STATUS_INDEX = 23
POWER_MANAGER_ERROR_MASK_INDEX = 24
REFORMER_ERROR_MASK_INDEX = 25
FUEL_CELL_ERROR_MASK_INDEX = 26


class FuelCellEngDataParticleType(BaseEnum):
    FUELCELL_ENG_DCL_RECOVERED = 'fuelcell_eng_dcl_recovered'
    FUELCELL_ENG_DCL_TELEMETERED = 'fuelcell_eng_dcl_telemetered'


class FuelCellEngDclParticleClassKey (BaseEnum):
    """
    An enum for the fuel cell engineering data particle class
    """
    ENGINEERING_DATA_PARTICLE_CLASS = 'engineering_data_particle_class'


class FuelCellEngDclParticleKey(BaseEnum):
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'
    DATALOG_MANAGER_VERSION = 'datalog_manager_version'
    SYSTEM_SOFTWARE_VERSION = 'system_software_version'
    TOTAL_RUN_TIME = 'total_run_time'
    FUEL_CELL_VOLTAGE = 'fuel_cell_voltage'
    FUEL_CELL_CURRENT = 'fuel_cell_current'
    REFORMER_TEMPERATURE = 'reformer_temperature'
    FUEL_CELL_H2_PRESSURE = 'fuel_cell_h2_pressure'
    FUEL_CELL_TEMPERATURE = 'fuel_cell_temperature'
    REFORMER_FUEL_PRESSURE = 'reformer_fuel_pressure'
    FUEL_PUMP_PWM_DRIVE_PERCENT = 'fuel_pump_pwm_drive_percent'
    AIR_PUMP_PWM_DRIVE_PERCENT = 'air_pump_pwm_drive_percent'
    COOLANT_PUMP_PWM_DRIVE_PERCENT = 'coolant_pump_pwm_drive_percent'
    AIR_PUMP_TACH_COUNT = 'air_pump_tach_count'
    FUEL_CELL_STATE = 'fuel_cell_state'
    FUEL_REMAINING = 'fuel_remaining'
    POWER_TO_BATTERY1 = 'power_to_battery1'
    BATTERY1_CONVERTER_TEMPERATURE = 'battery1_converter_temperature'
    POWER_TO_BATTERY2 = 'power_to_battery2'
    BATTERY2_CONVERTER_TEMPERATURE = 'battery2_converter_temperature'
    BALANCE_OF_PLANT_POWER = 'balance_of_plant_power'
    BALANCE_OF_PLANT_CONVERTER_TEMPERATURE = 'balance_of_plant_converter_temperature'
    POWER_BOARD_TEMPERATURE = 'power_board_temperature'
    CONTROL_BOARD_TEMPERATURE = 'control_board_temperature'
    POWER_MANAGER_STATUS = 'power_manager_status'
    POWER_MANAGER_ERROR_MASK = 'power_manager_error_mask'
    REFORMER_ERROR_MASK = 'reformer_error_mask'
    FUEL_CELL_ERROR_MASK = 'fuel_cell_error_mask'


class FuelCellEngDclDataCommonParticle(DataParticle):

    # dictionary for unpacking integer fields which map directly to a parameter
    
    UNPACK_DICT = {
        # Start at index 1, DCL timestamp is at 0
        'datalog_manager_version': 1,
        'system_software_version': 2,
        'total_run_time': 3,
        'fuel_cell_voltage': 4,
        'fuel_cell_current': 5,
        'reformer_temperature': 6,
        'fuel_cell_h2_pressure': 7,
        'fuel_cell_temperature': 8,
        'reformer_fuel_pressure': 9,
        'fuel_pump_pwm_drive_percent': 10,
        'air_pump_pwm_drive_percent': 11,
        'coolant_pump_pwm_drive_percent': 12,
        'air_pump_tach_count': 13,
        'fuel_cell_state': 14,
        'fuel_remaining': 15,
        'power_to_battery1': 16,
        'battery1_converter_temperature': 17,
        'power_to_battery2': 18,
        'battery2_converter_temperature': 19,
        'balance_of_plant_power': 20,
        'balance_of_plant_converter_temperature': 21,
        'power_board_temperature': 22,
        'control_board_temperature': 23,
        'power_manager_status': 24,
        'power_manager_error_mask': 25,
        'reformer_error_mask': 26,
        'fuel_cell_error_mask': 27
    }

    def _build_parsed_values(self):

        particle_parameters = [self._encode_value('dcl_controller_timestamp',
                                                  self.raw_data['dcl_controller_timestamp'], str)]

        # Loop through the unpack dictionary and encode integers
        for name, index in self.UNPACK_DICT.iteritems():
            particle_parameters.append(self._encode_value(name, self.raw_data[name], int))

        return particle_parameters


class FuelCellEngDclDataParticleRecovered(FuelCellEngDclDataCommonParticle):
    _data_particle_type = FuelCellEngDataParticleType.FUELCELL_ENG_DCL_RECOVERED
    

class FuelCellEngDclDataParticleTelemetered(FuelCellEngDclDataCommonParticle):
    _data_particle_type = FuelCellEngDataParticleType.FUELCELL_ENG_DCL_TELEMETERED


class FuelCellEngDclParser(SimpleParser):
    """
    Class used to parse the fuelcell_eng_dcl dataset.
    """
    def __init__(self,
                 config,
                 file_handle,
                 exception_callback):

        self._end_of_file = False
        self._file_handle = file_handle

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)

            # Set the metadata and data particle classes to be used later

            if FuelCellEngDclParticleClassKey.ENGINEERING_DATA_PARTICLE_CLASS in particle_classes_dict:

                self._fuelcell_data_class = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    FuelCellEngDclParticleClassKey.ENGINEERING_DATA_PARTICLE_CLASS]
            else:
                log.error(
                    'Configuration missing engineering data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing engineering data particle class key in particle classes dict')
        else:
            log.error('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

        super(FuelCellEngDclParser, self).__init__(config, file_handle, exception_callback)

    def log_warning(self, msg_text, which_line):
        """
        :param msg_text: The text to display in the log
        :param which_line: The line number where the problem occurred
        """
        self._exception_callback(
            RecoverableSampleException(msg_text + ' %d - No particle generated', which_line))

    @staticmethod
    def good_field(field_array):
        """
        Check to see if there are the correct number of fields and that
        each field in the array field contains an integer string
        :param field_array: The array of fuel cell data
        """
        if len(field_array) != 27:
            return False

        for x in range(0, len(field_array)):
            # First check for a greater than zero length field
            if len(field_array[x]) > 0:
                # The account for the possibility of a minus sign
                # which would make the field non-digits but would
                # still be a valid field Example '-12345' is valid.
                if field_array[x].startswith('-'):
                    start_char = 1
                else:
                    start_char = 0
            else:
                return False

            if not str.isdigit(field_array[x][start_char:]):
                return False

        return True

    @staticmethod
    def good_checksum(data_array, read_checksum):
        """
        Calculate the checksum from the read in data and compare it to the
        checksum contained in the data
        :param data_array: the array of fuelcell data
        :param read_checksum: the checksum contained in the data
        :return: True if the checksum is bad, otherwise false
        """
        calculated_checksum = 0
        for x in range(0, len(data_array)):
            calculated_checksum += ord(data_array[x])

            # Modulo 32768 is applied to the checksum to keep it a 16 bit value
            calculated_checksum %= 32768

        if calculated_checksum == read_checksum:
            return True
        else:
            return False

    @staticmethod
    def get_timestamp(my_tuple):
        """
        Convert the date and time from the record to a Unix timestamp
        :param my_tuple: The timestamp as a tuple
        :return: the NTP timestamp
        """
        timestamp = my_tuple[:6]+(0, 0, 0)
        elapsed_seconds = calendar.timegm(timestamp) + int(my_tuple[6])/1000.0

        return float(ntplib.system_to_ntp_time(elapsed_seconds))

    @staticmethod
    def generate_data_dict(datetime, data_list):
        """
        Pull the needed fields from the data line and convert them
        to the format needed for the particle per the IDD. Then put
        that data in a dictionary
        :param datetime: the DCL timestamp
        :param data_list: the array of fuelcell data 
        :return: The dictionary
        """
        datalog_manager_version = data_list[DATALOG_MANAGER_VERSION_INDEX]
        system_software_version = data_list[SYSTEM_SOFTWARE_VERSION_INDEX]
        total_run_time = data_list[TOTAL_RUN_TIME_INDEX]
        fuel_cell_voltage = data_list[FUEL_CELL_VOLTAGE_INDEX]
        fuel_cell_current = data_list[FUEL_CELL_CURRENT_INDEX]
        reformer_temperature = data_list[REFORMER_TEMPERATURE_INDEX]
        fuel_cell_h2_pressure = data_list[FUEL_CELL_H2_PRESSURE_INDEX]
        fuel_cell_temperature = data_list[FUEL_CELL_TEMPERATURE_INDEX]
        reformer_fuel_pressure = data_list[REFORMER_FUEL_PRESSURE_INDEX]
        fuel_pump_pwm_drive_percent = data_list[FUEL_PUMP_PWM_DRIVE_PERCENT_INDEX]
        air_pump_pwm_drive_percent = data_list[AIR_PUMP_PWM_DRIVE_PERCENT_INDEX]
        coolant_pump_pwm_drive_percent = data_list[COOLANT_PUMP_PWM_DRIVE_PERCENT_INDEX]
        air_pump_tach_count = data_list[AIR_PUMP_TACH_COUNT_INDEX]
        fuel_cell_state = data_list[FUEL_CELL_STATE_INDEX]
        fuel_remaining = data_list[FUEL_REMAINING_INDEX]
        power_to_battery1 = data_list[POWER_TO_BATTERY1_INDEX]
        battery1_converter_temperature = data_list[BATTERY1_CONVERTER_TEMPERATURE_INDEX]
        power_to_battery2 = data_list[POWER_TO_BATTERY2_INDEX]
        battery2_converter_temperature = data_list[BATTERY2_CONVERTER_TEMPERATURE_INDEX]
        balance_of_plant_power = data_list[BALANCE_OF_PLANT_POWER_INDEX]
        balance_of_plant_converter_temperature = data_list[BALANCE_OF_PLANT_CONVERTER_TEMPERATURE_INDEX]
        power_board_temperature = data_list[POWER_BOARD_TEMPERATURE_INDEX]
        control_board_temperature = data_list[CONTROL_BOARD_TEMPERATURE_INDEX]
        power_manager_status = data_list[POWER_MANAGER_STATUS_INDEX]
        power_manager_error_mask = data_list[POWER_MANAGER_ERROR_MASK_INDEX]
        reformer_error_mask = data_list[REFORMER_ERROR_MASK_INDEX]
        fuel_cell_error_mask = data_list[FUEL_CELL_ERROR_MASK_INDEX]

        return {FuelCellEngDclParticleKey.DCL_CONTROLLER_TIMESTAMP: datetime,
                FuelCellEngDclParticleKey.DATALOG_MANAGER_VERSION: datalog_manager_version,
                FuelCellEngDclParticleKey.SYSTEM_SOFTWARE_VERSION: system_software_version,
                FuelCellEngDclParticleKey.TOTAL_RUN_TIME: total_run_time,
                FuelCellEngDclParticleKey.FUEL_CELL_VOLTAGE: fuel_cell_voltage,
                FuelCellEngDclParticleKey.FUEL_CELL_CURRENT: fuel_cell_current,
                FuelCellEngDclParticleKey.REFORMER_TEMPERATURE: reformer_temperature,
                FuelCellEngDclParticleKey.FUEL_CELL_H2_PRESSURE: fuel_cell_h2_pressure,
                FuelCellEngDclParticleKey.FUEL_CELL_TEMPERATURE: fuel_cell_temperature,
                FuelCellEngDclParticleKey.REFORMER_FUEL_PRESSURE: reformer_fuel_pressure,
                FuelCellEngDclParticleKey.FUEL_PUMP_PWM_DRIVE_PERCENT: fuel_pump_pwm_drive_percent,
                FuelCellEngDclParticleKey.AIR_PUMP_PWM_DRIVE_PERCENT: air_pump_pwm_drive_percent,
                FuelCellEngDclParticleKey.COOLANT_PUMP_PWM_DRIVE_PERCENT: coolant_pump_pwm_drive_percent,
                FuelCellEngDclParticleKey.AIR_PUMP_TACH_COUNT: air_pump_tach_count,
                FuelCellEngDclParticleKey.FUEL_CELL_STATE: fuel_cell_state,
                FuelCellEngDclParticleKey.FUEL_REMAINING: fuel_remaining,
                FuelCellEngDclParticleKey.POWER_TO_BATTERY1: power_to_battery1,
                FuelCellEngDclParticleKey.BATTERY1_CONVERTER_TEMPERATURE: battery1_converter_temperature,
                FuelCellEngDclParticleKey.POWER_TO_BATTERY2: power_to_battery2,
                FuelCellEngDclParticleKey.BATTERY2_CONVERTER_TEMPERATURE: battery2_converter_temperature,
                FuelCellEngDclParticleKey.BALANCE_OF_PLANT_POWER: balance_of_plant_power,
                FuelCellEngDclParticleKey.BALANCE_OF_PLANT_CONVERTER_TEMPERATURE: balance_of_plant_converter_temperature,
                FuelCellEngDclParticleKey.POWER_BOARD_TEMPERATURE: power_board_temperature,
                FuelCellEngDclParticleKey.CONTROL_BOARD_TEMPERATURE: control_board_temperature,
                FuelCellEngDclParticleKey.POWER_MANAGER_STATUS: power_manager_status,
                FuelCellEngDclParticleKey.POWER_MANAGER_ERROR_MASK: power_manager_error_mask,
                FuelCellEngDclParticleKey.REFORMER_ERROR_MASK: reformer_error_mask,
                FuelCellEngDclParticleKey.FUEL_CELL_ERROR_MASK: fuel_cell_error_mask}

    def parse_file(self):
        """
        Parser for velpt_ab_dcl data.
        """
        line_count = 0

        # Read a single line from the input file
        fuelcell_input_row = self._file_handle.readline()

        # Read the file, one line at a time
        while fuelcell_input_row:

            line_count += 1

            # Check to see if this record contains fuel cell data
            if not NON_DATA_MATCHER.search(fuelcell_input_row):

                # Is the record properly time stamped?
                found_dtg = DATE_MATCHER.search(fuelcell_input_row)

                # If so, continue processing
                if found_dtg:

                    # Grab the time stamp data from the data
                    dtg = found_dtg.group(1)

                    # Now get the fuel cell data from the input line
                    found_data = START_DATA_MATCHER.search(fuelcell_input_row)

                    # If an integer was found, followed by a comma, the line has fuel cell data.
                    if found_data:

                        data_string = fuelcell_input_row[found_data.start(1)+1:]

                        # Need to find the colon near the end of the line which marks the
                        # end of the actual fuel cell data. The colon marks the end of the
                        # fuel cell data followed by the checksum for that data. Following
                        # that there will be a space then a hexadecimal number. If any of those
                        # elements are missing, the data is suspect.
                        found_end = END_DATA_MATCHER.search(data_string)

                        if found_end:

                            # First, split the data at any spaces there should be only one space!
                            data_part = data_string.split(' ')

                            # If the end of the data line is properly formatted, there will only
                            # be two pieces after the split: the comma delimited data with the
                            # colon delimited checksum and the line terminator (4 digit hex number.)
                            # However, if there is an extraneous space in the comma delimited data,
                            # we need to drop just the last part of the split and pack all the others
                            # back together.
                            if len(data_part) == 2:  # The nominal case
                                the_data = data_part[0]
                            else:  # The abnormal case (need to rebuild the space separate parts minus the last part
                                the_data = data_part[0]
                                for x in range(1, len(data_part) - 1):
                                    the_data += data_part[x]

                            data_plus_checksum = the_data.split(':')
                            actual_data = data_plus_checksum[0]
                            read_checksum = int(data_plus_checksum[1])

                            if self.good_checksum(actual_data, read_checksum):
                                the_fields = actual_data.split(',')

                                if self.good_field(the_fields):
                                    timestamp = self.get_timestamp((int(found_dtg.group(YEAR_GROUP)),
                                                                    int(found_dtg.group(MONTH_GROUP)),
                                                                    int(found_dtg.group(DAY_GROUP)),
                                                                    int(found_dtg.group(HOUR_GROUP)),
                                                                    int(found_dtg.group(MINUTE_GROUP)),
                                                                    int(found_dtg.group(SECONDS_GROUP)),
                                                                    int(found_dtg.group(MILLISECONDS_GROUP))))

                                    fuelcell_data_dict = self.generate_data_dict(dtg, the_fields)
                                    particle = self._extract_sample(self._fuelcell_data_class,
                                                                    None,
                                                                    fuelcell_data_dict,
                                                                    timestamp)

                                    self._record_buffer.append(particle)

                                else:
                                    self.log_warning('Improper format line', line_count)
                            else:
                                self.log_warning('Bad checksum line', line_count)
                        else:
                            self.log_warning('No terminator found on line', line_count)
                    else:
                        self.log_warning('No data found on line', line_count)
                else:
                    self.log_warning('Bad/Missing Timestamp on line', line_count)
            else:
                self.log_warning('No fuel cell data on line', line_count)

            # Read another line from the input file
            fuelcell_input_row = self._file_handle.readline()
