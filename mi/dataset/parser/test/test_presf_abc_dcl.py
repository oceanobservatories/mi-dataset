#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser

"""

import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import \
    ParserUnitTestCase, \
    BASE_RESOURCE_PATH

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.presf_abc_dcl import \
    PresfAbcDclRecoveredTideDataParticle, \
    PresfAbcDclTelemeteredTideDataParticle, \
    PresfAbcDclRecoveredWaveDataParticle, \
    PresfAbcDclTelemeteredWaveDataParticle, \
    DataTypeKey, \
    TIDE_PARTICLE_CLASS_KEY, \
    WAVE_PARTICLE_CLASS_KEY, \
    PresfAbcDclParser

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH,
                             'presf_abc',
                             'dcl', 'resource')

MODULE_NAME = 'mi.dataset.parser.presf_abc_dcl'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class PresfAbcDclParserUnitTestCase(ParserUnitTestCase):
    """
    presf_abc_dcl Parser unit test suite
    """
    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def open_file(self, filename):
        openfile = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return openfile

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._exception_occurred = False
        
        self.config = {
            DataTypeKey.PRESF_ABC_DCL_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    TIDE_PARTICLE_CLASS_KEY: PresfAbcDclTelemeteredTideDataParticle,
                    WAVE_PARTICLE_CLASS_KEY: PresfAbcDclTelemeteredWaveDataParticle,
                }
            },
            DataTypeKey.PRESF_ABC_DCL_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    TIDE_PARTICLE_CLASS_KEY: PresfAbcDclRecoveredTideDataParticle,
                    WAVE_PARTICLE_CLASS_KEY: PresfAbcDclRecoveredWaveDataParticle,
                }
            },
        }

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

        self.maxDiff = None

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE =====')

        with open(os.path.join(RESOURCE_PATH, '20140417.presf3.log'), 'r') as file_handle:

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)
            
            # file has one tide particle and one wave particle
            particles = parser.get_records(2)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)
       
            self.assert_particles(particles, '20140417.presf3_telem.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140417.presf3.log'), 'r') as file_handle:

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_RECOVERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)
            
            # file has one tide particle and one wave particle
            particles = parser.get_records(2)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 2 particles
            self.assertTrue(len(particles) == 2)
       
            self.assert_particles(particles, '20140417.presf3_recov.yml', RESOURCE_PATH)

        log.debug('===== END TEST SIMPLE =====')

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST MANY =====')
        with open(os.path.join(RESOURCE_PATH, '20140105_trim.presf.log'), 'r') as file_handle:

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)
            
            particles = parser.get_records(20)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 20)
    
            self.assert_particles(particles, "20140105_trim.presf_telem.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140105_trim.presf.log'), 'r') as file_handle:

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_RECOVERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)
            
            particles = parser.get_records(20)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 20)
    
            self.assert_particles(particles, "20140105_trim.presf_recov.yml", RESOURCE_PATH)

        log.debug('===== END TEST MANY =====')

    def test_long_stream(self):
        """
        Test a long stream
        """
        log.debug('===== START TEST LONG STREAM =====')
        with open(os.path.join(RESOURCE_PATH, '20140105.presf.log'), 'r') as file_handle:

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)
    
            particles = parser.get_records(48)
                
            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            # Make sure we obtained 20 particles
            self.assertTrue(len(particles) == 48)

        log.debug('===== END TEST LONG STREAM =====')

    def test_invalid_tide_record(self):
        """
        The file used here has a damaged tide record ( the p = $$$ is replaced by a q = $$$ )
        """
        log.debug('===== START TEST INVALID TIDE RECORD =====')

        with open(os.path.join(RESOURCE_PATH, '20140105_invts.presf.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 20
            NUM_EXPECTED_PARTICLES = 19

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20140105_invts.presf_telem.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
 
        with open(os.path.join(RESOURCE_PATH, '20140105_invts.presf.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 20
            NUM_EXPECTED_PARTICLES = 19

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_RECOVERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20140105_invts.presf_recov.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

        log.debug('===== END TEST INVALID TIDE RECORD =====')

    def test_invalid_wave_record(self):
        """
        Two of the wave records in this file are damaged.  The first is missing the pt subrecord,
        and the second is missing the termination of the wave record.
        """
        log.debug('===== START TEST INVALID WAVE RECORD =====')

        with open(os.path.join(RESOURCE_PATH, '20140105_invwv.presf.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 20
            NUM_EXPECTED_PARTICLES = 18

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)
            
            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20140105_invwv.presf_telem.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
 
        with open(os.path.join(RESOURCE_PATH, '20140105_invwv.presf.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 20
            NUM_EXPECTED_PARTICLES = 18

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_RECOVERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assert_particles(particles, "20140105_invwv.presf_recov.yml", RESOURCE_PATH)

            for i in range(len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
 
        log.debug('===== END TEST INVALID TIDE RECORD =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file
        has no instrument records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, '20140417.presf1.log'), 'r') as file_handle:

            NUM_PARTICLES_TO_REQUEST = 10
            NUM_EXPECTED_PARTICLES = 0

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)

            particles = parser.get_records(NUM_PARTICLES_TO_REQUEST)

            # Make sure there were no errors
            self.assertTrue(len(self.exception_callback_value) == 0)

            self.assertEquals(len(particles), NUM_EXPECTED_PARTICLES)

            self.assertEqual(self._exception_occurred, False)

        log.debug('===== END TEST NO PARTICLES =====')

    def test_Bug_3021(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE =====')

        with open(os.path.join(RESOURCE_PATH, '20141204.presf.log'), 'r') as file_handle:

            parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_TELEMETERED),
                                       None, file_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       self.exception_callback)

            # file has one tide particle and one wave particle
            particles = parser.get_records(20)

            # Make sure there was one error due to the "time out"
            self.assertTrue(len(self.exception_callback_value) == 1)

            # Make sure we obtained 0 particles
            self.assertEquals(len(particles), 0)



