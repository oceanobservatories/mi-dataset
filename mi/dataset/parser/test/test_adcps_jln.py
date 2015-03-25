#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@stream_handle marine-integrations/mi/dataset/parser/test/test_adcps_jln.py
@author Jeff Roy
@brief Test code for a adcps_jln data parser
Parts of this test code were taken from test_adcpa.py
Due to the nature of the records in PD0 files, (large binary records with hundreds of parameters)
this code verifies select items in the parsed data particle
"""

from nose.plugins.attrib import attr
import yaml
import numpy
import os

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset',
                             'driver', 'adcps_jln', 'stc', 'resource')


@attr('UNIT', group='mi')
class AdcpsJlnParserUnitTestCase(ParserUnitTestCase):
    """
    adcps_jln Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcps_jln',
                       DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpsJlnParticle'}
        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        # test01 data was all manually verified against the IDD
        # and double checked with PD0decoder_v2 MATLAB tool
        self.test01 = {}
        self.test01['internal_timestamp'] = 3581719370.030000
        self.test01['ensemble_start_time'] = 3581719370.0299997329711914
        self.test01['echo_intensity_beam1'] = [89, 51, 44, 43, 43, 43, 43, 44, 43, 44, 43, 43, 44, 44, 44,
                                               43, 43, 44, 43, 44, 44, 43, 43, 44, 44, 44, 44, 44, 44, 44,
                                               43, 43, 43, 43, 43, 43, 43, 44, 44, 43, 44, 44, 43, 43, 44,
                                               43, 43, 44, 44, 43, 43, 44, 43, 43, 44]
        self.test01['correlation_magnitude_beam1'] = [68, 70, 18, 19, 17, 17, 20, 19, 17, 15, 17, 20, 16,
                                                      17, 16, 17, 17, 18, 18, 17, 17, 19, 18, 17, 17, 19,
                                                      19, 17, 16, 16, 18, 19, 19, 17, 19, 19, 19, 18, 20,
                                                      17, 19, 19, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.test01['percent_good_3beam'] = [53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.test01['water_velocity_east'] = [383, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                              -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                              -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                              -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                              -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                              -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                              -32768, -32768, -32768, -32768, -32768, -32768, -32768]
        self.test01['water_velocity_north'] = [314, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                               -32768, -32768, -32768, -32768, -32768, -32768, -32768]
        self.test01['water_velocity_up'] = [459, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                            -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                            -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                            -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                            -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                            -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                            -32768]
        self.test01['error_velocity'] = [80, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                         -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                         -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                         -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                         -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                         -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768, -32768,
                                         -32768]
        # test02 data was extracted using the PD0decoder_v2 MATLAB tool
        # ensemble 1 of file ADCP_CCE1T_20.000
        self.test02 = {}
        self.test02['ensemble_number'] = 1
        self.test02['real_time_clock'] = [12, 9, 21, 0, 0, 0, 0]
        self.test02['heading'] = 21348
        self.test02['pitch'] = 4216
        self.test02['roll'] = 3980

        # test03 data was extracted using the PD0decoder_v2 MATLAB tool
        # ensemble 20 of file ADCP_CCE1T_20.000
        self.test03 = {}
        self.test03['ensemble_number'] = 20
        self.test03['real_time_clock'] = [12, 9, 21, 19, 0, 0, 0]
        self.test03['heading'] = 538
        self.test03['pitch'] = 147
        self.test03['roll'] = 221

    def assert_result(self, test, particle):
        """
        Suite of tests to run against each returned particle and expected
        results of the same.  The test parameter should be a dictionary
        that contains the keys to be tested in the particle
        the 'internal_timestamp' and 'position' keys are
        treated differently than others but can be verified if supplied
        """

        particle_dict = particle.generate_dict()

        # for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get('values'):
            particle_values[param['value_id']] = param['value']

        # compare each key in the test to the data in the particle
        for key in test:
            test_data = test[key]

            # get the correct data to compare to the test
            if key == 'internal_timestamp':
                particle_data = particle.get_value('internal_timestamp')
                # the timestamp is in the header part of the particle
            elif key == 'position':
                particle_data = self.state_callback_value['position']
                # position corresponds to the position in the file
            else:
                particle_data = particle_values.get(key)
                # others are all part of the parsed values part of the particle

            if particle_data is None:
                # generally OK to ignore index keys in the test data, verify others

                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:
                if isinstance(test_data, float):
                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    self.assertTrue(compare)
                else:
                    # otherwise they are all ints and should be exactly equal
                    self.assertEqual(test_data, particle_data)

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ADCP_data_20130702.PD0 has one record in it
        with open(os.path.join(RESOURCE_PATH, 'ADCP_data_20130702.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(1)

            # this simple test shows the 2 ways to verify results
            self.assert_result(self.test01, particles[0])

            self.assert_particles(particles, 'ADCP_data_20130702.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        # ADCP_CCE1T_20.000 has 20 records in it
        with open(os.path.join(RESOURCE_PATH, 'ADCP_CCE1T_20.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(20)

            self.assert_result(self.test02, particles[0])
            self.assert_result(self.test03, particles[19])

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # ADCP_data_Corrupted.PD0 has one bad record followed by one good in it
        with open(os.path.join(RESOURCE_PATH, 'ADCP_data_Corrupted.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(1)
            self.assert_result(self.test01, particles[0])

    def test_long_stream(self):
        """
        Verify an entire file against a yaml result file.
        """
        with open(os.path.join(RESOURCE_PATH, 'ADCP_CCE1T_20.000'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(20)

            self.assert_particles(particles, 'ADCP_CCE1T_20.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])
