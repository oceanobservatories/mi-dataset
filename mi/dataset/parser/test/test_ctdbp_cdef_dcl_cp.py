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
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.test.test_parser import ParserUnitTestCase

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_cdef_dcl_cp import \
    CtdbpCdefDclCpRecoveredParserDataParticle, \
    CtdbpCdefDclCpTelemeteredParserDataParticle, \
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
    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataParticleType.INSTRUMENT_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: CtdbpCdefDclCpTelemeteredParserDataParticle,
            },
            DataParticleType.INSTRUMENT_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: CtdbpCdefDclCpRecoveredParserDataParticle,
            },
        }

        self.state_callback_value = None
        self.publish_callback_value = None

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE =====')

        # test along the telemetered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)

            # Get a single data record using the telemetered path
            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20131123.ctdbp1_1rec.yml', RESOURCE_PATH)

        # test the recovered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
        
            # grab a record from the recovered path
            particles = parser.get_records(1)
        
        # Make sure we obtained 2 particles
        self.assertTrue(len(particles) == 1)
        
        self.assert_particles(particles, '20131123.ctdbp1_1rec_r.yml', RESOURCE_PATH)
        
        # test the corrected file format, use the recovered path
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_1rec_c.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
        
            # Grab a record of the corrected format, using the recovered path
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
                                          file_handle,
                                          self.exception_callback)
            particles = parser.get_records(24)

            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_telemetered.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
            particles = parser.get_records(24)
            # Make sure we obtained 24 particles
            self.assertTrue(len(particles) == 24)
            self.assert_particles(particles, "20131123.ctdbp1_many_recovered.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_corrected.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                           file_handle,
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
                                          file_handle,
                                          self.exception_callback)
    
            particles = parser.get_records(3389)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 3389)

        log.debug('===== END TEST LONG STREAM =====')

    def test_invalid_record(self):
        """
        The file used here has a damaged record ( missing datum )
        """
        log.debug('===== START TEST INVALID RECORD =====')

        # check error handling on an uncorrected data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_1inval.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 24
            NUM_EXPECTED_PARTICLES = 23

            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                      file_handle,
                      self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20131123.ctdbp1_many_recovered_1inval.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))


        # similarly, check error handling on a truncated, corrected file
        with open(os.path.join(RESOURCE_PATH, '20131123.ctdbp1_many_corrected_1inval.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 24
            NUM_EXPECTED_PARTICLES = 23

            parser = CtdbpCdefDclCpParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                      file_handle,
                      self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20131123.ctdbp1_many_recovered_1inval.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        log.debug('===== END TEST INVALID RECORD =====')


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
                      file_handle,
                      self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)
	    
        log.debug('===== END TEST NO PARTICLES =====')



