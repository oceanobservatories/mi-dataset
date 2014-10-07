#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser

"""

import os
import numpy
import ntplib
import yaml

from nose.plugins.attrib import attr

from mi.idk.config import Config

from mi.core.log import get_logger ; log = get_logger()
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import DataParticleKey

from mi.dataset.test.test_parser import ParserUnitTestCase

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_cdef_dcl_cp import \
    CtdbpCdefDclCpRecoveredParserDataParticle, \
    CtdbpCdefDclCpTelemeteredParserDataParticle, \
    StateKey, \
    DataParticleType, \
    PARTICLE_CLASS_KEY, \
    CtdbpCdefDclCpParser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                 'dataset', 'driver', 'ctdbp_cdef',
                 'dcl_cp', 'resource')

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl_cp'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class CtdbpCdefDclCpParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_dcl_cp Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Callback method to watch what comes in via the exception callback """
        log.debug(exception)
        self.exception_callback_value = exception
        self._exception_occurred = True
        self.count += 1

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._exception_occurred = False

        self.config = {
            DataParticleType.INSTRUMENT_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    PARTICLE_CLASS_KEY: CtdbpCdefDclCpTelemeteredParserDataParticle,
                }
            },
            DataParticleType.INSTRUMENT_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    PARTICLE_CLASS_KEY: CtdbpCdefDclCpRecoveredParserDataParticle,
                }
            },
        }

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None
        self.exception_callback_value = None
        self.count = 0

        self.maxDiff = None

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE =====')

        # test along the telemetered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20131123.ctdbp1_1rec.yml', RESOURCE_PATH)

        # test the recovered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(1)

        # Make sure we obtained 2 particles
        self.assertTrue(len(particles) == 1)
       
        self.assert_particles(particles, '20131123.ctdbp1_1rec_r.yml', RESOURCE_PATH)

        # test the corrected file format, use the recovered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec_c.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(1)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 1)
       
            self.assert_particles(particles, '20131123.ctdbp1_1rec_r.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST MANY =====')
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)
            particles = parser.get_records(24)

        # Make sure we obtained 24 particles
        self.assertTrue(len(particles) == 24)
        self.assert_particles(particles, "20131123.ctdbp1_many_telemetered.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)
            particles = parser.get_records(24)
            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_recovered.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_corrected.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)
            particles = parser.get_records(24)
            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_recovered.yml", RESOURCE_PATH)

        log.debug('===== END TEST MANY =====')

    def test_long_stream(self):
        """
        Test a long stream
        """
        log.debug('===== START TEST LONG STREAM =====')
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1.log'), 'r') as file_handle:

            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          None, file_handle,
                                          self.state_callback, self.pub_callback,
                                          self.exception_callback)
    
            particles = parser.get_records(3389)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 3389)

        log.debug('===== END TEST LONG STREAM =====')

    def test_invalid_record(self):
        """
        The file used here has a damaged tide record ( the p = $$$ is replaced by a q = $$$ )
        """
        log.debug('===== START TEST INVALID RECORD =====')

        # check error handling on an uncorrected data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_1inval.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 24
            NUM_EXPECTED_PARTICLES = 23

            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                      None, file_handle,
                      self.state_callback, self.pub_callback,
                      self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20131123.ctdbp1_many_recovered_1inval.yml", RESOURCE_PATH)

            self.assertEqual(self._exception_occurred, True)

        # similarly, check error handling on a truncated, corrected file
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_corrected_1inval.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 24
            NUM_EXPECTED_PARTICLES = 23

            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                      None, file_handle,
                      self.state_callback, self.pub_callback,
                      self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20131123.ctdbp1_many_recovered_1inval.yml", RESOURCE_PATH)

            self.assertEqual(self._exception_occurred, True)

        log.debug('===== END TEST INVALID TIDE RECORD =====')


    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_0rec.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 0

            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                      None, file_handle,
                      self.state_callback, self.pub_callback,
                      self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assertEqual(self._exception_occurred, False)

        log.debug('===== END TEST NO PARTICLES =====')



