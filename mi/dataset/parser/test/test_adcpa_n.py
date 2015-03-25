#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_adcpa_n.py
@author Jeff Roy
@brief Test code for a adcpa_n data parser

"""

from nose.plugins.attrib import attr
import os

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset',
                             'driver', 'adcpa_n', 'resource')


@attr('UNIT', group='mi')
class AdcpNParserUnitTestCase(ParserUnitTestCase):
    """
    AdcpNParser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpa_n',
                       DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpaNInstrumentParticle'}

    def test_simple(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp_auv_3.pd0'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(5)  # ask for 5 should get 3

            log.info('got back %d particles', len(particles))

            # Note yaml file was generated but hand checked against output of
            # vendor supplied MATLAB tool outputs
            self.assert_particles(particles, 'adcp_auv_3.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 0)

    def test_get_many(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp_auv_51.pd0'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(51)

            log.info('got back %d particles', len(particles))

            # Note yaml file was generated but hand checked against output of
            # vendor supplied MATLAB tool outputs
            self.assert_particles(particles, 'adcp_auv_51.yml', RESOURCE_PATH)
            self.assertEqual(len(self.exception_callback_value), 0)

    def test_long_stream(self):

        with open(os.path.join(RESOURCE_PATH, 'adcp.adc'), 'rb') as stream_handle:
            parser = AdcpPd0Parser(self.config, stream_handle, self.exception_callback)

            particles = parser.get_records(5000)  # ask for 5000 should get 4132

            log.debug('got back %d particles', len(particles))
            self.assertEqual(len(particles), 4132)
            self.assertEqual(self.exception_callback_value, [])

