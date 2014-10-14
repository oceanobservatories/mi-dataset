#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser

"""

import os

from nose.plugins.attrib import attr

from mi.idk.config import Config

from mi.core.exceptions import \
    RecoverableSampleException

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_cdef_dcl_ce import \
    CtdbpCdefDclCeRecoveredParserDataParticle, \
    CtdbpCdefDclCeTelemeteredParserDataParticle, \
    CtdbpCdefDclCeRecoveredParserDostaParticle, \
    CtdbpCdefDclCeTelemeteredParserDostaParticle, \
    DataParticleType, \
    PARTICLE_CLASS_KEY, \
    DOSTA_CLASS_KEY, \
    CtdbpCdefDclCeParser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                             'dataset', 'driver', 'ctdbp_cdef',
                             'dcl_ce', 'resource')

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl_ce'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class CtdbpCdefDclCeParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_dcl_ce Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataParticleType.INSTRUMENT_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    PARTICLE_CLASS_KEY: CtdbpCdefDclCeTelemeteredParserDataParticle,
                    DOSTA_CLASS_KEY: CtdbpCdefDclCeTelemeteredParserDostaParticle,
                }
            },
            DataParticleType.INSTRUMENT_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    PARTICLE_CLASS_KEY: CtdbpCdefDclCeRecoveredParserDataParticle,
                    DOSTA_CLASS_KEY: CtdbpCdefDclCeRecoveredParserDostaParticle,
                }
            },
        }

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== TEST SIMPLE: UNCORR TELEM =====')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_1rec_uncorr.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(1)

            # Make sure we obtained 1 particle
            self.assertTrue(len(particles) == 1)
            self.assert_particles(particles, '20140918.ctdbp_1rec_uncorr_t.yml', RESOURCE_PATH)

        log.debug('===== TEST SIMPLE: UNCORR RECOV =====')
        # test the recovered path, current config
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_1rec_uncorr.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
        
            # file has one tide particle and one wave particle
            particles = parser.get_records(1)
        
        # Make sure we obtained 1 particle
        self.assertTrue(len(particles) == 1)    
        self.assert_particles(particles, '20140918.ctdbp_1rec_uncorr_r.yml', RESOURCE_PATH)
    
        # test the corrected file format, use the recovered path
        # first varient  ( corrrected formats deliver 2 particles per record )
        log.debug('===== TEST SIMPLE: CORR V1 =====')
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_1rec_corr1st.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
        
            # file has one data particle and one dosta particle
            particles = parser.get_records(2)
        
            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)
            self.assert_particles(particles, '20140930.ctdbp1_1rec_corr.yml', RESOURCE_PATH)
            
        # test the corrected file format, use the recovered path
        # second varient
        log.debug('===== TEST SIMPLE: CORR V2 =====')
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_1rec_corr2nd.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
        
            # file has one tide particle and one wave particle
            particles = parser.get_records(2)
        
            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)
            self.assert_particles(particles, '20140930.ctdbp1_1rec_corr.yml', RESOURCE_PATH)
        
        log.debug('===== END TEST SIMPLE =====')
        
    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST MANY =====')
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_many.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)
            particles = parser.get_records(14)

        # Make sure we obtained 24 particles
        self.assertTrue(len(particles) == 14)
        self.assert_particles(particles, "20140918.ctdbp_many_uncorr_t.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant_many.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
            particles = parser.get_records(14)
            # Make sure we obtained 14 particles
            self.assertTrue(len(particles) == 14)
            self.assert_particles(particles, "20140930.ctdbp1_many_corr.yml", RESOURCE_PATH)
        
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr2ndVariant_many.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)
            particles = parser.get_records(14)
            # Make sure we obtained 14 particles
            self.assertTrue(len(particles) == 14)
            self.assert_particles(particles, "20140930.ctdbp1_many_corr.yml", RESOURCE_PATH)

        log.debug('===== END TEST MANY =====')

    def test_long_stream(self):
        """
        Test a long stream
        """
        log.debug('===== START TEST LONG STREAM =====')
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)
    
            particles = parser.get_records(291)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 291)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)
    
            particles = parser.get_records(18)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 18)

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr2ndVariant.log'), 'r') as file_handle:
            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)
    
            particles = parser.get_records(18)

            # Make sure we obtained 3389 particles
            self.assertTrue(len(particles) == 18)

        log.debug('===== END TEST LONG STREAM =====')

    def test_invalid_record(self):
        """
        The file used here has a damaged tide record ( missing datum )
        """
        log.debug('===== START TEST INVALID RECORD =====')

        # check error handling on an uncorrected data file ( one record truncated )
        with open(os.path.join(RESOURCE_PATH, '20140918.ctdbp_many_broken.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 14
            NUM_EXPECTED_PARTICLES = 13

            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)
            self.assert_particles(particles, "20140918.ctdbp_many_uncorr_t_broken.yml", RESOURCE_PATH)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        # similarly, check error handling on a truncated, corrected file
        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_corr1stVariant_many_broken.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 14
            NUM_EXPECTED_PARTICLES = 12

            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_RECOVERED),
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20140930.ctdbp1_many_corr_broken.yml", RESOURCE_PATH)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        log.debug('===== END TEST INVALID TIDE RECORD =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, '20140930.ctdbp1_0rec_corr2nd.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 0

            parser = CtdbpCdefDclCeParser(self.config.get(DataParticleType.INSTRUMENT_TELEMETERED),
                                          file_handle,
                                          self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)
            self.assertEquals(len(self.exception_callback_value), 0)
            
        log.debug('===== END TEST NO PARTICLES =====')



