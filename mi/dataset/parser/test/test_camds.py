#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_camds.py
@author Dan Mergens
@brief Test code for a camds data parser
"""

import os

from nose.plugins.attrib import attr
from mi.core.log import get_logger
from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.camds import \
    CamdsHtmlParser, CamdsParserDataParticleType
from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

log = get_logger()

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'camds', 'resource')

SAMPLE_DATA = ['20150816T160224,178', '20100716T001808,321', '20100101T140322,378', '20150219T170005,545',
               '20160420T191456,943', '20160923T080447,820']

SAMPLE_PARTICLE_DICT = [
    {'value_id': 'camds_brightness', 'value': 40},
    {'value_id': 'camds_focus_position', 'value': 14},
    {'value_id': 'camds_gain', 'value': 0},
    {'value_id': 'camds_iris_position', 'value': 0},
    {'value_id': 'camds_pan_position', 'value': 0},
    {'value_id': 'camds_tilt_position', 'value': 0},
    {'value_id': 'filepath', 'value': '20100101T140322,378.png'},
    {'value_id': 'internal_timestamp', 'value': 3471343402.378}
]


@attr('UNIT', group='mi')
class CamdsParserUnitTestCase(ParserUnitTestCase):
    """
    camds_abc_dcl Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.camds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CamdsMetadataParticle',
        }

    def test_simple(self):
        """

        """
        for root in SAMPLE_DATA:
            data = root + '.htm'
            yml = root + '.yml'
            file_path = os.path.join(RESOURCE_PATH, data)
            # stream_handle = open(file_path, 'r')
            with open(file_path, 'r') as stream_handle:

                parser = CamdsHtmlParser(stream_handle, self.exception_callback)

                particles = parser.get_records(1)

            log.debug("*** test_simple Num particles %s", len(particles))

            self.assert_particles(particles, yml, RESOURCE_PATH)

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists and a RecoverableSampleException is thrown.
        Note: every other data record has bad data (float instead of int, extra column etc.)
        We don't have any examples of corrupt html from the CAMDS, so we'll just try to read in one of the YAML files
        instead of using a known corrupt file.
        """

        file_path = os.path.join(RESOURCE_PATH, SAMPLE_DATA[0] + '.yml')
        with open(file_path, 'rU') as stream_handle:

            with self.assertRaises(KeyError):
                parser = CamdsHtmlParser(stream_handle, self.exception_callback)

                parser.get_records(1)
