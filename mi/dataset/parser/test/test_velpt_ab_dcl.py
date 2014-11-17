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

        self._incomplete_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self._bad_parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {}
        }

    def test_simple(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        This is the happy path.
        """
        log.debug('===== START TEST SIMPLE =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

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
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'too_few_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_few_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'too_few_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

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
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'too_many_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'too_many_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'too_many_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

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
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'extra_bytes_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

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
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_id_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, '20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_id_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID RECORD ID =====')

    def test_bad_diagnostic_checksum(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD CHECKSUM =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diagnostic_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diagnostic_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_diagnostic_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diagnostic_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD CHECKSUM =====')

    def test_truncated_file(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND TRUNCATED FILE =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'truncated_file_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'truncated_file_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'truncated_file_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

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
                                          self.exception_callback)

        log.debug('===== END TEST BAD CONFIGURATION =====')

    def test_bad_velocity_checksum(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 49 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST FOUND BAD VELOCITY CHECKSUM =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_velocity_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_velocity_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_velocity_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_velocity_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD VELOCITY CHECKSUM =====')

    def test_diag_header_bad_checksum(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 491 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST FOUND BAD DIAGNOSTIC HEADER CHECKSUM =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diag_hdr_checksum_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAGNOSTIC HEADER CHECKSUM =====')

    def test_missing_diag_header(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 491 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST MISSING DIAGNOSTIC HEADER =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_header_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'missing_diag_header_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_header_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 49

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_missing_diag_header_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING DIAGNOSTIC HEADER =====')

    def test_random_diag_record(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 491 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST FOUND RANDOM DIAGNOSTIC RECORD =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'random_diag_record_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'random_diag_record_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'random_diag_record_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 51

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_random_diag_record_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST FOUND RANDOM DIAGNOSTIC RECORD =====')

    def test_missing_diag_recs(self):
        """
        The file used in this test has a record with a bad checksum.
        This results in 49 particles being retrieved instead of 50.
        The standard 20140813.velpt.log was used, the checksum of the
        third velocity record was corrupted to make it fail.
        """
        log.debug('===== START TEST MISSING DIAGNOSTIC RECORDS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_recs_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 29

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'missing_diag_recs_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('----- RECOVERED -----')
        with open(os.path.join(RESOURCE_PATH, 'missing_diag_recs_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 29

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_missing_diag_recs_20140813.velpt.yml', RESOURCE_PATH)

        log.debug('===== END TEST MISSING DIAGNOSTIC RECORDS =====')

    def test_partial_configuration(self):
        """
        Attempt to build a parser with a bad configuration.
        """
        log.debug('===== START TEST PARTIAL CONFIGURATION =====')

        with open(os.path.join(RESOURCE_PATH, '20140813.velpt.log'), 'rb') as file_handle:

            with self.assertRaises(ConfigurationException):
                parser = VelptAbDclParser(self._incomplete_parser_config,
                                          file_handle,
                                          self.exception_callback)

        log.debug('===== END TEST PARTIAL CONFIGURATION =====')

    def test_bad_diag_checksum_19_recs(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD DIAG HDR CHECKSUM AND TOO FEW RECS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_19_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 48

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_19_diag_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_19_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 48

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diag_hdr_checksum_19_diag_20140813.velpt.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAG HDR CHECKSUM AND TOO FEW RECS =====')

    def test_bad_diag_checksum_21_recs(self):
        """
        The file used in this test has a power record with a missing timestamp.
        This results in 9 particles being retrieved instead of 10, and also result in the exception
        callback being called.
        """
        log.debug('===== START TEST FOUND BAD DIAG HDR CHECKSUM AND TOO MANY RECS =====')

        # Test the telemetered version
        log.debug('----- TELEMETERED -----')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_21_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._telemetered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'bad_diag_hdr_checksum_21_diag_20140813.velpt.yml', RESOURCE_PATH)

        # Test the recovered version
        log.debug('------ RECOVERED ------')
        with open(os.path.join(RESOURCE_PATH, 'bad_diag_hdr_checksum_21_diag_20140813.velpt.log'), 'rb') as file_handle:

            num_particles_to_request = num_expected_particles = 50

            parser = VelptAbDclParser(self._recovered_parser_config,
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles, 'recovered_bad_diag_hdr_checksum_21_diag_20140813.velpt.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST FOUND BAD DIAG HDR CHECKSUM AND TOO MANY RECS =====')

