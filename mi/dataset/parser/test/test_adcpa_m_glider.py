#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_adcpa_m_glider.py
@author Jeff Roy
@brief Test code for a adcpa_m_glider data parser
Parts of this test code were taken from test_adcpa.py
Due to the nature of the records in PD0 files, (large binary records with hundreds of parameters)
this code verifies select items in the parsed data particle
"""

from nose.plugins.attrib import attr
import os

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import RecoverableSampleException

from mi.idk.config import Config
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset',
                             'driver', 'moas', 'gl', 'adcpa', 'resource')


@attr('UNIT', group='mi')
class AdcpsMGliderParserUnitTestCase(ParserUnitTestCase):
    """
    AdcpMGlider Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recov = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpa_m_glider',
                             DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpaMGliderRecoveredParticle'}

        self.config_telem = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpa_m_glider',
                             DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpaMGliderInstrumentParticle'}

    def test_simple_recov(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, 'ND072022.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)
            particles = parser.get_records(1)
    
            log.debug('got back %d particles', len(particles))
    
            self.assert_particles(particles, 'ND072022_recov.yml', RESOURCE_PATH)

    def test_simple_telem(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, 'ND072022.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_telem, stream_handle, self.exception_callback)

            particles = parser.get_records(1)

            log.debug('got back %d particles', len(particles))

            self.assert_particles(particles, 'ND072022_telem.yml', RESOURCE_PATH)

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'ND072023.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(54)
            log.debug('got back %d records', len(particles))

            self.assert_particles(particles, 'ND072023_recov.yml', RESOURCE_PATH)

    def test_with_status_data(self):
        """
        Verify the parser will work with a file that contains the status data block
        This was found during integration test with real recovered data
        """

        with open(os.path.join(RESOURCE_PATH, 'ND161646.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)

            particles = parser.get_records(250)
            log.debug('got back %d records', len(particles))

            self.assert_particles(particles, 'ND161646.yml', RESOURCE_PATH)

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # LB180210_3_corrupted.PD0 has three records in it, the 2nd record was corrupted
        with open(os.path.join(RESOURCE_PATH, 'LB180210_3_corrupted.PD0'), 'rb') as stream_handle:

            parser = AdcpPd0Parser(self.config_recov, stream_handle, self.exception_callback)

            # try to get 3 particles, should only get 2 back
            # the second one should correspond to ensemble 3
            parser.get_records(3)

            log.debug('Exceptions : %s', self.exception_callback_value[0])

            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

