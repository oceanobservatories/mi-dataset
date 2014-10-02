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

import re
import binascii
from datetime import datetime
import time
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

from mi.core.log import get_logger
log = get_logger()


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

END_OF_LINE_REGEX = r'(?:\r\n|\n)'   # any line termination
DCL_DATE_REGEX = r'\d{4}/\d{2}/\d{2}'      # Date: YYYY/MM/DD
DCL_TIME_REGEX = r'\d{2}:\d{2}:\d{2}.\d{3}'      # Time: HH:MM:SS.ddd
SPACE = ' '        # just a space

# capture the DCL timestamp
# include trailing space so they can be completely removed from the input record
DCL_TIMESTAMP_REGEX = '(' + DCL_DATE_REGEX + SPACE + DCL_TIME_REGEX + SPACE + ')'
# beginning of every PD0 ensemble
PD0_HEADER_REGEX = r'7[Ff]7[Ff]'
# non-capturing 2 character representation of a hex byte
ASCII_HEX_BYTE_REGEX = r'(?:[0-9a-fA-F]{2})'
# PD0 Header followed by any number of bytes
PD0_ENSEMBLE_REGEX = '(' + PD0_HEADER_REGEX + ASCII_HEX_BYTE_REGEX + r'+)'


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

        dcl_controller_timestamp = self._dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_TIMESTAMP]
        dcl_controller_start_timestamp = self._dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_STARTING_TIMESTAMP]

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

        # the timestamp comes from the DCL logger timestamp, parse the string into a datetime
        dcl_datetime = datetime.strptime(dcl_controller_timestamp, "%Y/%m/%d %H:%M:%S.%f")
        # adjust local seconds to get to utc by subtracting timezone seconds
        utc_time = float(dcl_datetime.strftime("%s.%f")) - time.timezone

        self.set_internal_timestamp(unix_time=utc_time)

        return parsed_values


class AdcptAcfgmPd0DclInstrumentParticle(AdcptAcfgmPd0DclParticle):

    #set the data_particle_type for the DataParticle class
    _data_particle_type = DataParticleType.ADCPT_ACFGM_PD0_DCL_INSTRUMENT


class AdcptAcfgmPd0DclInstrumentRecoveredParticle(AdcptAcfgmPd0DclParticle):

    #set the data_particle_type for the DataParticle class
    _data_particle_type = DataParticleType.ADCPT_ACFGM_PD0_DCL_INSTRUMENT_RECOVERED


class AdcptAcfgmDclPd0Parser(Parser):
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

        line = self._stream_handle.readline()

        # Go through each line in the file
        while line:

            #find  all the DCL timestamps in the line.
            dcl_timestamps_match = re.findall(DCL_TIMESTAMP_REGEX, line)

            #remove all DCL timestamps
            stripped_line = re.sub(DCL_TIMESTAMP_REGEX, '', line)
            #remove the line termination
            stripped_line = re.sub(END_OF_LINE_REGEX, '', stripped_line)

            # only need to process the lines with PD0 data, all others can be ignored
            if re.match(PD0_ENSEMBLE_REGEX, stripped_line):

                # convert the stripped PD0 hex ascii back to binary
                pd0_data = binascii.unhexlify(stripped_line)

                # make sure it is a valid record
                if self._validate_checksum(pd0_data):

                    # create a dictionary as raw_data parameter to extract_sample
                    dcl_data_dict = {}

                    # save the binary pd0 data
                    dcl_data_dict[AdcptAcfgmPd0DclKey.PD0_DATA] = pd0_data

                    # strip off the trailing space and save the last DCL timestamp group in raw data
                    dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_TIMESTAMP] = dcl_timestamps_match[-1][:-1]
                    # strip off the trailing space and save the first DCL timestamp group in raw data
                    dcl_data_dict[AdcptAcfgmPd0DclKey.DCL_CONTROLLER_STARTING_TIMESTAMP] = dcl_timestamps_match[0][:-1]

                    # providing dcl_data_dict as raw data parameter
                    # the adcpt afgm particle class will pop off the pd0
                    # data and provide it to the base pd0 particle class
                    particle = self._extract_sample(self._particle_class,
                                                    None,
                                                    dcl_data_dict,
                                                    None)

                    self._record_buffer.append(particle)

            line = self._stream_handle.readline()

        # provide an indication that the file was parsed
        self._file_parsed = True
        log.debug('PARSE_FILE create %s particles', len(self._record_buffer))

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

        num_bytes = struct.unpack("<H", input_buffer[2: 4])[0]
        # get the number of bytes in the record, number of bytes is immediately
        # after the sentinel bytes and does not include the 2 checksum bytes

        record_start = 0
        record_end = num_bytes

        #if there is enough in the buffer check the record
        if record_end <= len(input_buffer[0: -CHECKSUM_BYTES]):
            #make sure the checksum bytes are in the buffer too

            total = 0
            for i in range(record_start, record_end):
                total += ord(input_buffer[i])
            #add up all the bytes in the record

            checksum = total & CHECKSUM_MODULO  # bitwise and with 65535 or mod vs 65536

            #log.debug("checksum & total = %d %d ", checksum, total)

            if checksum == struct.unpack("<H", input_buffer[record_end: record_end + CHECKSUM_BYTES])[0]:
                return True
            else:
                err_msg = 'ADCPT ACFGM DCL RECORD FAILED CHECKSUM'
                self._exception_callback(RecoverableSampleException(err_msg))
                log.warn(err_msg)
                return False
