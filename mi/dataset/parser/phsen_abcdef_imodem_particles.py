#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/phsen_abcdef_imodem_particles.py
@author Joe Padula
@brief Particles for the phsen_abcdef_imodem recovered and telemetered dataset
Release notes:

initial release
"""

__author__ = 'jpadula'

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, DataParticleKey


class DataParticleType(BaseEnum):
    PHSEN_ABCDEF_IMODEM_INSTRUMENT = 'phsen_abcdef_imodem_instrument'
    PHSEN_ABCDEF_IMODEM_INSTRUMENT_RECOVERED = 'phsen_abcdef_imodem_instrument_recovered'
    PHSEN_ABCDEF_IMODEM_CONTROL = 'phsen_abcdef_imodem_control'
    PHSEN_ABCDEF_IMODEM_CONTROL_RECOVERED = 'phsen_abcdef_imodem_control_recovered'
    PHSEN_ABCDEF_IMODEM_METADATA = 'phsen_abcdef_imodem_metadata'
    PHSEN_ABCDEF_IMODEM_METADATA_RECOVERED = 'phsen_abcdef_imodem_metadata_recovered'


class PhsenAbcdefImodemDataParticleKey(BaseEnum):
    # Common to Instrument and Control data particles
    UNIQUE_ID = 'unique_id'                                 # PD353
    RECORD_TYPE = 'record_type'                             # PD355
    RECORD_TIME = 'record_time'                             # PD356
    VOLTAGE_BATTERY = 'voltage_battery'                     # PD358
    PASSED_CHECKSUM = 'passed_checksum'                     # PD2228

    # For instrument data particles
    THERMISTOR_START = 'thermistor_start'                   # PD932
    REFERENCE_LIGHT_MEASUREMENTS = 'reference_light_measurements'   # PD933
    LIGHT_MEASUREMENTS = 'light_measurements'               # PD357
    THERMISTOR_END = 'thermistor_end'                       # PD935

    # For control data particles
    CLOCK_ACTIVE = 'clock_active'                           # PD366
    RECORDING_ACTIVE = 'recording_active'                   # PD367
    RECORD_END_ON_TIME = 'record_end_on_time'               # PD368
    RECORD_MEMORY_FULL = 'record_memory_full'               # PD369
    RECORD_END_ON_ERROR = 'record_end_on_error'             # PD370
    DATA_DOWNLOAD_OK = 'data_download_ok'                   # PD371
    FLASH_MEMORY_OPEN = 'flash_memory_open'                 # PD372
    BATTERY_LOW_PRESTART = 'battery_low_prestart'           # PD373
    BATTERY_LOW_MEASUREMENT = 'battery_low_measurement'     # PD374
    BATTERY_LOW_BLANK = 'battery_low_blank'                 # PD2834
    BATTERY_LOW_EXTERNAL = 'battery_low_external'           # PD376
    EXTERNAL_DEVICE1_FAULT = 'external_device1_fault'       # PD377
    EXTERNAL_DEVICE2_FAULT = 'external_device2_fault'       # PD1113
    EXTERNAL_DEVICE3_FAULT = 'external_device3_fault'       # PD1114
    FLASH_ERASED = 'flash_erased'                           # PD378
    POWER_ON_INVALID = 'power_on_invalid'                   # PD379
    NUM_ERROR_RECORDS = 'num_error_records'                 # PD1116
    NUM_BYTES_STORED = 'num_bytes_stored'                   # PD1117

    # For control and metadata particles
    NUM_DATA_RECORDS = 'num_data_records'                   # PD1115

    # For metadata data particles
    FILE_TIME = 'file_time'                                 # PD3060
    INSTRUMENT_ID = 'instrument_id'                         # PD1089
    SERIAL_NUMBER = 'serial_number'                         # PD312
    VOLTAGE_FLT32 = 'voltage_flt32'                         # PD2649
    RECORD_LENGTH = 'record_length'                         # PD583
    NUM_EVENTS = 'num_events'                               # PD263
    NUM_SAMPLES = 'num_samples'                             # PD203


class PhsenAbcdefImodemScienceBaseDataParticle(DataParticle):
    """
    BaseDataParticle class for Science records, which are pH records or control records.
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = []

        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.UNIQUE_ID,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.UNIQUE_ID],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORD_TYPE,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORD_TYPE],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORD_TIME,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORD_TIME],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM],
                               int))

        return particle_params


class PhsenAbcdefImodemInstrumentDataParticle(PhsenAbcdefImodemScienceBaseDataParticle):
    """
    Class for the instrument particle.
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(PhsenAbcdefImodemInstrumentDataParticle, self)._build_parsed_values()

        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.THERMISTOR_START,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.THERMISTOR_START],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS],
                               list))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS],
                               list))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.THERMISTOR_END,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.THERMISTOR_END],
                               int))

        return particle_params


class PhsenAbcdefImodemControlDataParticle(PhsenAbcdefImodemScienceBaseDataParticle):
    """
    Class for the control particle.
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(PhsenAbcdefImodemControlDataParticle, self)._build_parsed_values()

        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.CLOCK_ACTIVE,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.CLOCK_ACTIVE],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORDING_ACTIVE,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORDING_ACTIVE],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_TIME,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_TIME],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORD_MEMORY_FULL,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORD_MEMORY_FULL],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_ERROR,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_ERROR],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.DATA_DOWNLOAD_OK,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.DATA_DOWNLOAD_OK],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.FLASH_MEMORY_OPEN,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.FLASH_MEMORY_OPEN],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_PRESTART,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_PRESTART],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_MEASUREMENT,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_MEASUREMENT],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_BLANK,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_BLANK],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_EXTERNAL,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_EXTERNAL],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE1_FAULT],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE2_FAULT],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE3_FAULT],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.FLASH_ERASED,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.FLASH_ERASED],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.POWER_ON_INVALID,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.POWER_ON_INVALID],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.NUM_ERROR_RECORDS,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.NUM_ERROR_RECORDS],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.NUM_BYTES_STORED,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.NUM_BYTES_STORED],
                               int))
        # battery voltage is optional in control record.
        if self.raw_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY]:
            particle_params.append(
                self._encode_value(PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                                   self.raw_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY],
                                   int))
        else:
            particle_params.append(
                {DataParticleKey.VALUE_ID: PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                 DataParticleKey.VALUE: None})
        return particle_params


class PhsenAbcdefImodemMetadataDataParticle(DataParticle):
    """
    Class for the metadata particle.
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = []

        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.FILE_TIME,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.FILE_TIME],
                               str))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID],
                               str))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER],
                               str))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32],
                               float))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.NUM_EVENTS,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.NUM_EVENTS],
                               int))
        particle_params.append(
            self._encode_value(PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES,
                               self.raw_data[PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES],
                               int))
        return particle_params


class PhsenAbcdefImodemInstrumentTelemeteredDataParticle(PhsenAbcdefImodemInstrumentDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_INSTRUMENT


class PhsenAbcdefImodemInstrumentRecoveredDataParticle(PhsenAbcdefImodemInstrumentDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_INSTRUMENT_RECOVERED


class PhsenAbcdefImodemControlTelemeteredDataParticle(PhsenAbcdefImodemControlDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_CONTROL


class PhsenAbcdefImodemControlRecoveredDataParticle(PhsenAbcdefImodemControlDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_CONTROL_RECOVERED


class PhsenAbcdefImodemMetadataTelemeteredDataParticle(PhsenAbcdefImodemMetadataDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_METADATA


class PhsenAbcdefImodemMetadataRecoveredDataParticle(PhsenAbcdefImodemMetadataDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_METADATA_RECOVERED
