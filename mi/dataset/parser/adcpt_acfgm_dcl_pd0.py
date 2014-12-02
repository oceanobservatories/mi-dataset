#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_acfgm_dcl_pd0
@file marine-integrations/mi/dataset/parser/adcpt_acfgm_dcl_pd0.py
@author Jeff Roy
@brief Particle and Parser classes for the adcpt_acfgm_dcl_pd0 drivers
The particles are parsed by the common PD0 Parser and
Abstract particle class in file adcp_pd0.py
Release notes:

initial release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

import binascii
import struct

from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import Parser
from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.parser.adcp_pd0 import \
    AdcpFileType, \
    AdcpPd0DataParticle, \
    CHECKSUM_BYTES, \
    CHECKSUM_MODULO
from mi.dataset.parser import utilities
from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()
METACLASS =get_logging_metaclass('trace')


class DataParticleType(BaseEnum):
    # Data particle types for the  ADCPT_ACFGM
    # PD0 format telemetered and recovered data files
    ADCPT_ACFGM_PD0_DCL_INSTRUMENT = 'adcpt_acfgm_pd0_dcl_instrument'
    ADCPT_ACFGM_PD0_DCL_INSTRUMENT_RECOVERED = 'adcpt_acfgm_pd0_dcl_instrument_recovered'


class AdcptAcfgmPd0DclKey(BaseEnum):
    # Enumerations for the additional DCL parameters in the adcpt_acfgm_pd0_dcl streams
    # The remainder of the streams are identical to the adcps_jln streams and
    # are handled by the base AdcpPd0DataParticle class
    # this enumeration is also used for the dcl_data_dict
    # of the particle class constructor
    # so includes the additional enumeration 'PD0_DATA'
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'
    DCL_CONTROLLER_STARTING_TIMESTAMP = 'dcl_controller_starting_timestamp'
    PD0_DATA = 'pd0_data'

PD0_START_STRING = '\x7f\x7f'


class AdcptAcfgmPd0DclParticle(AdcpPd0DataParticle):

    def __init__(self, dcl_data_dict,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        # set the file type for use by the AdcptAcfgmPd0DclParticle class
        # file_type must be set to a value in AdcpFileType
        file_type = AdcpFileType.ADCPS_File

        # pop off the PD0 data because it is not needed by this class
        pd0_data = dcl_data_dict.pop(AdcptAcfgmPd0DclKey.PD0_DATA)

        # provide only the PD0 data to the base particle class as raw data
        super(AdcptAcfgmPd0DclParticle, self).__init__(pd0_data,
                                                       port_timestamp,
                                                       internal_timestamp,
                                                       preferred_timestamp,
                                                       quality_flag,
                                                       new_sequence,
                                                       file_type)

        # save the rest of the dcl_data_dict
        self._dcl_data_dict = dcl_data_dict

    def _build_parsed_values(self):

        parsed_values = []  # initialize an empty list

        log.debug('ADCPT ACFGM BPV self._dcl_data_dict = %s', self._dcl_data_dict)

        dcl_controller_timestamp = \
            self._dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_TIMESTAMP]
        dcl_controller_start_timestamp = \
            self._dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_STARTING_TIMESTAMP]

        parsed_values.append(self._encode_value(AdcptAcfgmPd0DclKey.DCL_CONTROLLER_TIMESTAMP,
                                                dcl_controller_timestamp, str))

        parsed_values.append(self._encode_value(AdcptAcfgmPd0DclKey.DCL_CONTROLLER_STARTING_TIMESTAMP,
                                                dcl_controller_start_timestamp, str))

        # use the build_parsed values of the super class to parse all the pd0 data
        pd0_values = super(AdcptAcfgmPd0DclParticle, self)._build_parsed_values()

        # append the pd0 data to the dcl data
        parsed_values.extend(pd0_values)

        # Note: internal timestamp has to be saved after calling the super class
        # build_parsed_values to over-write what it has saved

        utc_time = utilities.dcl_controller_timestamp_to_utc_time(dcl_controller_timestamp)

        self.set_internal_timestamp(unix_time=utc_time)

        return parsed_values


class AdcptAcfgmPd0DclInstrumentParticle(AdcptAcfgmPd0DclParticle):

    #set the data_particle_type for the DataParticle class
    _data_particle_type = DataParticleType.ADCPT_ACFGM_PD0_DCL_INSTRUMENT


class AdcptAcfgmPd0DclInstrumentRecoveredParticle(AdcptAcfgmPd0DclParticle):

    #set the data_particle_type for the DataParticle class
    _data_particle_type = DataParticleType.ADCPT_ACFGM_PD0_DCL_INSTRUMENT_RECOVERED


class AdcptAcfgmDclPd0Parser(Parser):
    __metaclass__ = METACLASS

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,  # shouldn't be optional anymore
                 state_callback=None,  # No longer used
                 publish_callback=None):  # No longer used

        self._file_parsed = False
        self._record_buffer = []

        super(AdcptAcfgmDclPd0Parser, self).__init__(config,
                                                     stream_handle,
                                                     None,  # State no longer used
                                                     None,  # Sieve function no longer used
                                                     state_callback,
                                                     publish_callback,
                                                     exception_callback)

    def _parse_file(self):
        pd0_data = []
        dcl_data_dict = {}

        # Go through each line in the file
        for line in self._stream_handle:
            log.trace('line: %r', line)

            # data looks like this
            # TS [adcpt:DLOG4]:<data>
            # with the "label" sometimes missing.  We will split the line into tokens based on whitespace
            # The first token will always be the timestamp and the last token will sometimes be good
            # data.
            parts = line.strip().split()
            ts = ' '.join(parts[:2])
            data = parts[-1].split(':')[-1]

            # If we can decode it, assume it is good
            try:
                data = binascii.unhexlify(data)
            except TypeError:
                continue

            log.trace('Found valid data: %r', data)

            # Look for start of a PD0 ensemble.  If we have a particle queued up, ship it
            # then reset our state.
            if data.startswith(PD0_START_STRING):
                log.trace('found pd0 start')
                if self._create_particle(pd0_data, dcl_data_dict) or not dcl_data_dict:
                    dcl_data_dict = {
                        AdcptAcfgmPd0DclKey.DCL_CONTROLLER_STARTING_TIMESTAMP: ts,
                        AdcptAcfgmPd0DclKey.DCL_CONTROLLER_TIMESTAMP: ts
                    }
                    pd0_data = []

            pd0_data.append(data)
            dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_TIMESTAMP] = ts

        # finally, see if we have a particle now, if so, publish
        self._create_particle(pd0_data, dcl_data_dict)

        # provide an indication that the file was parsed
        self._file_parsed = True
        log.debug('PARSE_FILE create %s particles', len(self._record_buffer))

    def _create_particle(self, data, dcl_dict):
        # step through the list, see if there is a valid particle anywhere
        for i in xrange(len(data)):
            if data[i].startswith(PD0_START_STRING):
                if self._validate_checksum(''.join(data[i:])):
                    dcl_dict[AdcptAcfgmPd0DclKey.PD0_DATA] = ''.join(data)
                    particle = self._extract_sample(self._particle_class,
                                                    None,
                                                    dcl_dict,
                                                    None)
                    self._record_buffer.append(particle)
                    return True
        return False

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested > 0:

            # If the file was not read, let's parse it
            if self._file_parsed is False:
                self._parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return

    def _validate_checksum(self, input_buffer):

        num_bytes, = struct.unpack("<H", input_buffer[2:4])
        # get the number of bytes in the record, number of bytes is immediately
        # after the sentinel bytes and does not include the 2 checksum bytes

        if len(input_buffer) < (num_bytes+CHECKSUM_BYTES):
            # not enough bytes, fail
            log.info('Not enough bytes in buffer, fail checksum')
            return False

        checksum = sum([ord(x) for x in input_buffer[:num_bytes]]) & CHECKSUM_MODULO
        message_checksum, = struct.unpack("<H", input_buffer[num_bytes:num_bytes + CHECKSUM_BYTES])

        if checksum == message_checksum:
            return True

        err_msg = 'ADCPT ACFGM DCL RECORD FAILED CHECKSUM'
        self._exception_callback(RecoverableSampleException(err_msg))
        log.warn(err_msg)
        return False

