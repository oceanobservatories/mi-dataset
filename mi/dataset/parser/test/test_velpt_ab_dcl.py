#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_velpt_ab
@file mi-dataset/mi/dataset/parser/test/test_velpt_ab_dcl.py
@author Chris Goodrich
@brief Test code for the velpt_ab parser
"""

__author__ = 'Chris Goodrich'

from mi.logging import log
import os
from nose.plugins.attrib import attr
from mi.core.exceptions import ConfigurationException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.velpt_ab_dcl import VelptAbDclParser, VelptAbParticleClassKey
from mi.dataset.parser.velpt_ab_dcl_particles import VelptAbInstrumentDataParticle,\
    VelptAbDiagnosticsHeaderParticle, VelptAbDiagnosticsDataParticle, VelptAbInstrumentDataParticleRecovered,\
    VelptAbDiagnosticsHeaderParticleRecovered, VelptAbDiagnosticsDataParticleRecovered


from mi.idk.config import Config

RESOURCE_PATH = os.path.join(Config().base_dir(),
                             'mi', 'dataset', 'driver', 'velpt_ab', 'dcl','resource')


@attr('UNIT', group='mi')
class VelptAbDclParserUnitTestCase(ParserUnitTestCase):
    """
    velpt_ab_dcl Parser unit test suite
    """

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self._telemetered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDiagnosticsHeaderParticle,
                VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDiagnosticsDataParticle,
                VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbInstrumentDataParticle
            }
        }

        self._recovered_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDiagnosticsHeaderParticleRecovered,
                VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDiagnosticsDataParticleRecovered,
                VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbInstrumentDataParticleRecovered
            }
        }

        self._bad_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {}
        }

    def exception_callback(self, exception):
        log.debug(exception)
        self._exception_occurred = True

    def test_simple(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated.
        """
        log.debug('===== START TEST SIMPLE =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_too_few_diagnostics_records(self):
        """
        The file used in this test has only 19 diagnostics records in the second set.
        Twenty are expected.
        """
        log.debug('===== START TEST NOT ENOUGH DIAGNOSTICS RECORDS =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'too_few_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_few_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, 'too_few_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_too_few_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST NOT ENOUGH DIAGNOSTICS RECORDS =====')

    def test_too_many_diagnostics_records(self):
        """
        The file used in this test has 21 diagnostics records in the second set.
        Twenty are expected.
        """
        log.debug('===== START TEST TOO MANY DIAGNOSTICS RECORDS =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'too_many_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_many_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, 'too_many_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_too_many_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST TOO MANY DIAGNOSTICS RECORDS =====')

    def test_invalid_sync_byte(self):
        """
        The file used in this test has extra bytes between records which need to be skipped
        in order to process the correct number of particles.
        """
        log.debug('===== START TEST INVALID SYNC BYTE =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID SYNC BYTE =====')

    def test_invalid_record_id(self):
        """
        The file used in this test has extra bytes between records which need to be skipped
        in order to process the correct number of particles.
        """
        log.debug('===== START TEST INVALID RECORD ID =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'bad_id_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, 'bad_id_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID RECORD ID =====')

    def test_bad_checksum(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD CHECKSUM =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'bad_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, 'bad_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD CHECKSUM =====')

    def test_truncated_file(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND TRUNCATED FILE =====')

        # Test the telemetered version
        with open(os.path.join(RESOURCE_PATH, 'truncated_file_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'truncated_file_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        with open(os.path.join(RESOURCE_PATH, 'truncated_file_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback,
                                      None,
                                      None)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_truncated_file_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND TRUNCATED FILE =====')

    def test_bad_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST BAD CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = VelptAbDclParser(self._bad_parser_config,
                                          file_handle,
                                          self.exception_callback,
                                          None,
                                          None)

        log.debug('===== END TEST BAD CONFIGURATION =====')
