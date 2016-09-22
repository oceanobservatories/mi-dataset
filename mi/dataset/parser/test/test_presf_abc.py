#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc
@file marine-integrations/mi/dataset/parser/test/test_presf_abc.py
@author Christopher Fortin, Jeff Roy, Rene Gelinas
@brief Test code for a presf_abc data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import \
    ParserUnitTestCase, \
    BASE_RESOURCE_PATH

from mi.dataset.parser.presf_abc import PresfAbcParser

from mi.core.log import get_logger
log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'presf_abc',
                             'resource')

MODULE_NAME = 'mi.dataset.parser.presf_abc'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class PresfAbcParserUnitTestCase(ParserUnitTestCase):
    """
    presf_abc Parser unit test suite
    """

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE =====')

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_1.hex'), 'r') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)
            
            # file has one tide particle and one wave particle
            particles = parser.get_records(2)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)
       
            self.assert_particles(particles,
                                  'presf_abc_test_1.exp_results.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_big(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST BIG =====')
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_2.hex'), 'r') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(6)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 6 particles
            self.assertTrue(len(particles) == 6)

            self.assert_particles(particles,
                                  "presf_abc_test_2.exp_results.yml",
                                  RESOURCE_PATH)

        log.debug('===== END TEST BIG =====')

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST GET MANY =====')
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_3.hex'), 'r') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)
            
            particles = parser.get_records(20)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 20)

            self.assert_particles(particles,
                                  "presf_abc_test_3.exp_results.yml",
                                  RESOURCE_PATH)

        log.debug('===== END TEST GET MANY =====')

    def test_multiple_sessions(self):
        """
        Read test data from multiple sessions and pull out multiple data
        particles at one time. Assert that the results are those we expected.
        """
        log.debug('===== START TEST MULTIPLE SESSIONS =====')
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_4a.hex'), 'r') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(40)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 40)

            self.assert_particles(particles,
                                  "presf_abc_test_4a.exp_results.yml",
                                  RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_4b.hex'), 'r') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(8)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 8)

            self.assert_particles(particles,
                                  "presf_abc_test_4b.exp_results.yml",
                                  RESOURCE_PATH)

        log.debug('===== END TEST MULTIPLE SESSIONS =====')

    def test_invalid_tide_record(self):
        """
        The file used here has an invalid tide record.
        """
        log.debug('===== START TEST INVALID TIDE RECORD =====')

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_5.hex'), 'r') \
                as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles,
                                  "presf_abc_test_5.exp_results.yml",
                                  RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i],
                                        RecoverableSampleException))

        log.debug('===== END TEST INVALID TIDE RECORD =====')

    def test_invalid_wave_record(self):
        """
        Two of the wave records in this file are damaged.  The first is missing
        the pt subrecord, and the second is missing the termination of the wave
        record.
        """
        log.debug('===== START TEST INVALID WAVE RECORD =====')

        # Test the wrong number of wave burst records.
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_6a.hex'), 'r') \
                as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles,
                                  "presf_abc_test_6a.exp_results.yml",
                                  RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i],
                                        RecoverableSampleException))

        # Test an invalid format of a wave burst record.
        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_6b.hex'), 'r') \
                as file_handle:

            num_particles_to_request = 2
            num_expected_particles = 1

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            self.assertEquals(len(particles), num_expected_particles)

            self.assert_particles(particles,
                                  "presf_abc_test_6b.exp_results.yml",
                                  RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i],
                                        RecoverableSampleException))

        log.debug('===== END TEST INVALID WAVE RECORD =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_7.hex'), 'r') \
                as file_handle:

            num_particles_to_request = 10
            num_expected_particles = 0

            parser = PresfAbcParser(file_handle, self.exception_callback)

            particles = parser.get_records(num_particles_to_request)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            self.assertEquals(len(particles), num_expected_particles)

            self.assertEqual(len(self.exception_callback_value), 0)

        log.debug('===== END TEST NO PARTICLES =====')

    def test_invalid_session_data(self):
        """
        Verify that the session data check is executed.
        """
        log.debug('===== START TEST INVALID SESSION DATA =====')

        with open(os.path.join(RESOURCE_PATH, 'presf_abc_test_8.hex'), 'r') \
                as file_handle:

            parser = PresfAbcParser(file_handle, self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(2)

            # Make sure there was an exception found
            self.assertTrue(len(self.exception_callback_value) == 1)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)

            self.assert_particles(particles,
                                  'presf_abc_test_1.exp_results.yml',
                                  RESOURCE_PATH)

        log.debug('===== END TEST INVALID SESSION DATA =====')
