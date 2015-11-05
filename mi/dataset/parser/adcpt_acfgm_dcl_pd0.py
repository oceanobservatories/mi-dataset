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
import binascii

from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import Parser, DataSetDriverConfigKeys
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.parser import utilities
from mi.core.log import get_logger, get_logging_metaclass
from mi.dataset.parser.pd0_parser import AdcpPd0Record, PD0ParsingException, InsufficientDataException
import mi.dataset.parser.adcp_pd0 as adcp_pd0


__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

log = get_logger()
METACLASS = get_logging_metaclass('trace')


class DclKey(BaseEnum):
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


class Velocity(adcp_pd0.Pd0EarthParticle):
    def __init__(self, dcl_data_dict, *args, **kwargs):
        super(Velocity, self).__init__(*args, **kwargs)
        self._dcl_data_dict = dcl_data_dict

    def _build_parsed_values(self):
        utc_time = utilities.dcl_controller_timestamp_to_utc_time(
            self._dcl_data_dict[DclKey.DCL_CONTROLLER_TIMESTAMP])
        self.set_internal_timestamp(unix_time=utc_time)
        return super(Velocity, self)._build_parsed_values()


class Config(adcp_pd0.AdcpPd0AdcpsConfigParticle):
    def __init__(self, dcl_data_dict, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)
        self._dcl_data_dict = dcl_data_dict

    def _build_parsed_values(self):
        utc_time = utilities.dcl_controller_timestamp_to_utc_time(
            self._dcl_data_dict[DclKey.DCL_CONTROLLER_TIMESTAMP])
        self.set_internal_timestamp(unix_time=utc_time)
        return super(Config, self)._build_parsed_values()


class Engineering(adcp_pd0.AdcpPd0AdcpsEngineeringParticle):
    def __init__(self, dcl_data_dict, *args, **kwargs):
        super(Engineering, self).__init__(*args, **kwargs)
        self._dcl_data_dict = dcl_data_dict

    def _build_parsed_values(self):
        utc_time = utilities.dcl_controller_timestamp_to_utc_time(
            self._dcl_data_dict[DclKey.DCL_CONTROLLER_TIMESTAMP])
        self.set_internal_timestamp(unix_time=utc_time)
        return super(Engineering, self)._build_parsed_values()


class ErrorStatus(adcp_pd0.AdcpPd0AdcpsErrorStatusParticle):
    def __init__(self, dcl_data_dict, *args, **kwargs):
        super(ErrorStatus, self).__init__(*args, **kwargs)
        self._dcl_data_dict = dcl_data_dict

    def _build_parsed_values(self):
        utc_time = utilities.dcl_controller_timestamp_to_utc_time(
            self._dcl_data_dict[DclKey.DCL_CONTROLLER_TIMESTAMP])
        self.set_internal_timestamp(unix_time=utc_time)
        return super(ErrorStatus, self)._build_parsed_values()


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
        self._particle_classes = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT]
        self._particle_classes = {k: globals()[v] for k, v in self._particle_classes.iteritems()}
        self._last_values = {}

        super(AdcptAcfgmDclPd0Parser, self).__init__(config,
                                                     stream_handle,
                                                     None,  # State no longer used
                                                     None,  # Sieve function no longer used
                                                     state_callback,
                                                     publish_callback,
                                                     exception_callback)

    def _changed(self, particle):
        particle_dict = particle.generate_dict()
        stream = particle_dict.get('stream_name')
        values = particle_dict.get('values')
        last_values = self._last_values.get(stream)
        if values == last_values:
            return False

        self._last_values[stream] = values
        return True

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
                if self._create_particles(pd0_data, dcl_data_dict) or not dcl_data_dict:
                    dcl_data_dict = {
                        DclKey.DCL_CONTROLLER_STARTING_TIMESTAMP: ts,
                        DclKey.DCL_CONTROLLER_TIMESTAMP: ts
                    }
                    pd0_data = []

            pd0_data.append(data)
            dcl_data_dict[DclKey.DCL_CONTROLLER_TIMESTAMP] = ts

        # finally, see if we have a particle now, if so, publish
        self._create_particles(pd0_data, dcl_data_dict)

        # provide an indication that the file was parsed
        self._file_parsed = True
        log.debug('PARSE_FILE create %s particles', len(self._record_buffer))

    def _create_particles(self, data, dcl_dict):
        # step through the list, see if there is a valid particle anywhere
        for i in xrange(len(data)):
            if data[i].startswith(PD0_START_STRING):
                try:
                    pd0 = AdcpPd0Record(''.join(data[i:]))
                    velocity = self._particle_classes['velocity'](dcl_dict, pd0)
                    self._record_buffer.append(velocity)

                    config = self._particle_classes['config'](dcl_dict, pd0)
                    engineering = self._particle_classes['engineering'](dcl_dict, pd0)
                    error = self._particle_classes['error_status'](dcl_dict, pd0)

                    for particle in [config, engineering, error]:
                        if self._changed(particle):
                            self._record_buffer.append(particle)

                    return True
                except InsufficientDataException:
                    return False
                except PD0ParsingException as e:
                    self._exception_callback(RecoverableSampleException('Unable to parse PD0: %s' % e))
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
