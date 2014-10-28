#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_flord_l_wfp
@file marine-integrations/mi/dataset/parser/test/test_flord_l_wfp.py
@author Joe Padula
@brief Test code for a flord_l_wfp data parser (which uses the GlobalWfpEFileParser)
"""

from nose.plugins.attrib import attr

import yaml
import numpy
import os

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config
from mi.core.exceptions import SampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.WFP_E_file_common import StateKey
from mi.dataset.parser.flord_l_wfp import FlordLWfpInstrumentParserDataParticleKey

from mi.dataset.parser.global_wfp_e_file_parser import GlobalWfpEFileParser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver', 'flord_l_wfp', 'sio_mule', 'resource')


@attr('UNIT', group='mi')
class FlordLWfpParserUnitTestCase(ParserUnitTestCase):
    """
    Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Call back method to match what comes in via the exception callback """
        self.exception_callback_value = exception

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpInstrumentParserDataParticle'
        }

        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        self.start_state = {StateKey.POSITION: 0}

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

        self.test_particle1 = {}
        self.test_particle1['internal_timestamp'] = 3583638177.0
        self.test_particle1[StateKey.POSITION] = 204
        self.test_particle1[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_CHL] = 54
        self.test_particle1[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_BETA] = 112
        self.test_particle1[FlordLWfpInstrumentParserDataParticleKey.RAW_INTERNAL_TEMP] = 571
        self.test_particle1[FlordLWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649377

        self.test_particle2 = {}
        self.test_particle2['internal_timestamp'] = 3583638247
        self.test_particle2[StateKey.POSITION] = 414
        self.test_particle2[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_CHL] = 55
        self.test_particle2[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_BETA] = 112
        self.test_particle2[FlordLWfpInstrumentParserDataParticleKey.RAW_INTERNAL_TEMP] = 570
        self.test_particle2[FlordLWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649447

        self.test_particle3 = {}
        self.test_particle3['internal_timestamp'] = 3583638317
        self.test_particle3[StateKey.POSITION] = 624
        self.test_particle3[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_CHL] = 56
        self.test_particle3[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_BETA] = 114
        self.test_particle3[FlordLWfpInstrumentParserDataParticleKey.RAW_INTERNAL_TEMP] = 570
        self.test_particle3[FlordLWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649517

        self.test_particle4 = {}
        self.test_particle4['internal_timestamp'] = 3583638617
        self.test_particle4[StateKey.POSITION] = 1524
        self.test_particle4[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_CHL] = 54
        self.test_particle4[FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_BETA] = 110
        self.test_particle4[FlordLWfpInstrumentParserDataParticleKey.RAW_INTERNAL_TEMP] = 569
        self.test_particle4[FlordLWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP] = 1374649817

    def test_simple(self):
        """
        Read test data and pull out data particles six at a time.
        Assert that the results of sixth particle are those we expected.
        """

        file_path = os.path.join(RESOURCE_PATH, 'small.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, None, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        particles = parser.get_records(6)

        log.debug("particles: %s", particles)
        for particle in particles:
            log.info("*** test particle: %s", particle.generate_dict())

        # Make sure the sixth particle has the correct values
        test_data = self.get_dict_from_yml('good.yml')
        self.assert_result(test_data['data'][0], particles[5])

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, self.start_state, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        particles = parser.get_records(50)

        # Should end up with 20 particles
        self.assertTrue(len(particles) == 50)

        self.assert_particles(particles, 'E0000001_50.yml', RESOURCE_PATH)
        stream_handle.close()

    def test_long_stream(self):
        """
        Test a long stream
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, self.start_state, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        particles = parser.get_records(1000)
        # File is 20,530 bytes
        #   minus 24 header
        #   minus 16 footer
        #   each particle is 30 bytes
        # Should end up with 683 particles
        self.assertTrue(len(particles) == 683)
        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-DATA.DAT')
        stream_handle = open(file_path, 'rb')

        parser = GlobalWfpEFileParser(self.config, self.start_state, stream_handle,
                                      self.state_callback, self.pub_callback, self.exception_callback)

        with self.assertRaises(SampleException):
            parser.get_records(1)

        stream_handle.close()

    def test_bad_header(self):
        """
        Ensure that bad header is skipped when it exists.
        """

        # This case tests against a header that does not match
        # 0000 0000 0000 0100 0000 0000 0000 0151
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-HEADER1.DAT')
        stream_handle = open(file_path, 'rb')

        with self.assertRaises(SampleException):
            GlobalWfpEFileParser(self.config, self.start_state, stream_handle,
                                 self.state_callback, self.pub_callback, self.exception_callback)

        stream_handle.close()

        # This case tests against a header that does not match global E-type data, but matches coastal
        # E-type data: 0001 0000 0000 0000 0001 0001 0000 0000
        file_path = os.path.join(RESOURCE_PATH, 'E0000001-BAD-HEADER2.DAT')
        stream_handle = open(file_path, 'rb')

        with self.assertRaises(SampleException):
            GlobalWfpEFileParser(self.config, self.start_state, stream_handle,
                                 self.state_callback, self.pub_callback, self.exception_callback)

        stream_handle.close()

    def assert_result(self, test, particle):
        """
        Suite of tests to run against each returned particle and expected
        results of the same.  The test parameter should be a dictionary
        that contains the keys to be tested in the particle
        the 'internal_timestamp' and 'position' keys are
        treated differently than others but can be verified if supplied
        """
        log.debug("test arg: %s", test)
        particle_dict = particle.generate_dict()
        log.debug("particle_dict: %s", particle_dict)
        #for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get('values'):
            particle_values[param['value_id']] = param['value']

        # compare each key in the test to the data in the particle
        for key in test:
            test_data = test[key]

            #get the correct data to compare to the test
            if key == 'internal_timestamp':
                particle_data = particle.get_value('internal_timestamp')
                #the timestamp is in the header part of the particle

                log.info("internal_timestamp %d", particle_data)

            elif key == 'position':
                particle_data = self.state_callback_value['position']
                #position corresponds to the position in the file

                log.info("position %d", particle_data)

            else:
                particle_data = particle_values.get(key)
                #others are all part of the parsed values part of the particle

            if particle_data is None:
                #generally OK to ignore index keys in the test data, verify others

                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:
                if isinstance(test_data, float):
                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    self.assertTrue(compare)
                else:
                    # otherwise they are all ints and should be exactly equal
                    log.debug("test_data %s, particle_data %s", test_data, particle_data)
                    self.assertEqual(test_data, particle_data)

    @staticmethod
    def get_dict_from_yml(filename):
        """
        This utility routine loads the contents of a yml file
        into a dictionary
        """

        fid = open(os.path.join(RESOURCE_PATH, filename), 'r')
        result = yaml.load(fid)
        fid.close()

        return result

