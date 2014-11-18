#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_flord_l_wfp.py
@author Maria Lutz, Mark Worden
@brief Test code for a flord_l_wfp_sio data parser
"""
import os
import ntplib, struct
from datetime import datetime
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import UnexpectedDataException
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys, DataParticleKey
from mi.dataset.parser.flord_l_wfp_sio import FlordLWfpSioParser

from mi.idk.config import Config

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                             'dataset', 'driver', 'flord_l_wfp',
                             'sio', 'resource')

@attr('UNIT', group='mi')
class FlordLWfpSioParserUnitTestCase(ParserUnitTestCase):
    """
    flord_l_wfp_sio Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpSioDataParticle'
        }


    def timestamp_to_ntp(self, hex_timestamp):
        fields = struct.unpack('>I', hex_timestamp)
        timestamp = float(fields[0])
        return ntplib.system_to_ntp_time(timestamp)

    def convert_utc_to_zulu(self, hex_timestamp):
        fields = struct.unpack('>I', hex_timestamp)
        timestamp = float(fields[0])
        zulu_time = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return zulu_time

    def test_parsing_of_input_file_without_decimation_factor(self):
        """
        This test method will process a flord_l_wfp_sio input file that does not include a status
        particle with a decimation factor.
        """
        file_path = os.path.join(RESOURCE_PATH, 'node58p1_0.we_wfp.dat')

        # Open the file holding the test sample data
        with open(file_path, 'rb') as stream_handle:

            self.parser = FlordLWfpSioParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = self.parser.get_records(1000)

            # We should end up with 160 particles
            self.assertTrue(len(particles) == 160)

            self.assert_particles(particles, 'node58p1_0.we_wfp.yml', RESOURCE_PATH)

    def test_parsing_of_input_file_with_decimation_factor(self):
        """
        This test method will process a flord_l_wfp_sio input file that includes a status
        particle with a decimation factor.
        """
        file_path = os.path.join(RESOURCE_PATH, 'node58p1_3.we_wfp.dat')

        # Open the file holding the test sample data
        with open(file_path, 'rb') as stream_handle:

            self.parser = FlordLWfpSioParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = self.parser.get_records(1000)

            # We should end up with 49 particles
            self.assertTrue(len(particles) == 49)

            self.assert_particles(particles, 'node58p1_3.we_wfp.yml', RESOURCE_PATH)

    def test_parsing_of_input_file_with_unexpected_data(self):
        """
        This test method will process a flord_l_wfp_sio input file that includes unexpected data.
        """
        file_path = os.path.join(RESOURCE_PATH, 'flord_l_wfp_sio_unexpected_data.dat')

        # Open the file holding the test sample data
        with open(file_path, 'rb') as stream_handle:

            self.parser = FlordLWfpSioParser(self.config, stream_handle, self.exception_callback)

            # Attempt to retrieve 1000 particles
            particles = self.parser.get_records(1000)

            self.assertEquals(len(self.exception_callback_value), 3)

            for i in range(0,len(self.exception_callback_value)):
                self.assert_(isinstance(self.exception_callback_value[i], UnexpectedDataException))

            # We should end up with 0 particles
            self.assertTrue(len(particles) == 0)
