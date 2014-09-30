#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_pco_2a_dcl
@file marine-integrations/mi/dataset/parser/test/test_pco_2a_dcl.py
@author Sung Ahn
@brief Test code for a pco_2a_dcl data parser

Files used for testing:

20140217.pco2a.log
  Sensor Data - 216 records

"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger

log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.pco_a_2a_dcl import \
    Pco2aDclAirParser, \
    Pco2aDclAirRecoveredParser

from mi.idk.config import Config

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver',
                             'pco_2a', 'dcl', 'resource')

MODULE_NAME = 'mi.dataset.parser.pco_a_dcl'
FILE = '20140217.pco2a.log'
FILE_FAILURE = '20140217.pco2a_failure.log'

YAML_FILE = 'rec_20140217_pco2a.yml'
YAML_FILE_REC = 'rec_20140217_pco2a_rec.yml'

RECORDS = 216  # number of records expected


@attr('UNIT', group='mi')
class Pco2aDclParserUnitTestCase(ParserUnitTestCase):
    """
    pco_2a_dcl Parser unit test suite
    """

    def create_rec_parser(self, file_handle, new_state=None):
        """
        This function creates a POC2ADcl parser for recovered data.
        """
        parser = Pco2aDclAirRecoveredParser(self.rec_config,
                                            file_handle, new_state, self.rec_state_callback,
                                            self.rec_pub_callback, self.rec_exception_callback)
        return parser

    def create_tel_parser(self, file_handle, new_state=None):
        """
        This function creates a PCO2ADcl parser for telemetered data.
        """
        parser = Pco2aDclAirParser(self.tel_config,
                                   file_handle, new_state, self.rec_state_callback,
                                   self.tel_pub_callback, self.tel_exception_callback)
        return parser

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def rec_state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.rec_state_callback_value = state
        self.rec_file_ingested_value = file_ingested

    def tel_state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.tel_state_callback_value = state
        self.tel_file_ingested_value = file_ingested

    def rec_pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.rec_publish_callback_value = pub

    def tel_pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.tel_publish_callback_value = pub

    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.rec_exception_callback_value = exception
        self.rec_exceptions_detected += 1

    def tel_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.tel_exception_callback_value = exception
        self.tel_exceptions_detected += 1

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.tel_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.rec_state_callback_value = None
        self.rec_file_ingested_value = False
        self.rec_publish_callback_value = None
        self.rec_exception_callback_value = None
        self.rec_exceptions_detected = 0

        self.tel_state_callback_value = None
        self.tel_file_ingested_value = False
        self.tel_publish_callback_value = None
        self.tel_exception_callback_value = None
        self.tel_exceptions_detected = 0

        self.maxDiff = None

    def test_verify_record(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are there.
        """
        log.debug('===== START TEST verify record parser =====')
        in_file = self.open_file(FILE)
        parser = self.create_tel_parser(in_file)

        parser.get_records(RECORDS)
        self.assertEqual(self.tel_exception_callback_value, None)

        record = parser.get_records(1)
        self.assertNotEqual(record, None)

        in_file.close()
        log.debug('===== END TEST verify record parser =====')

    def test_verify_parser(self):
        """
        Read Telemetered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains metadata entries.
        This test includes testing metadata entries as well
        """
        log.debug('===== START TEST verify parser =====')
        in_file = self.open_file(FILE)
        parser = self.create_tel_parser(in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        self.assertEqual(self.tel_exception_callback_value, None)

        in_file.close()
        log.debug('===== END TEST verify parser =====')

    def test_verify_record_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are there.

        """
        log.debug('===== START TEST verify_parser RECOVERED =====')
        in_file = self.open_file(FILE)
        parser = self.create_rec_parser(in_file)

        parser.get_records(RECORDS)

        self.assertEqual(self.rec_exception_callback_value, None)

        record = parser.get_records(1)
        self.assertNotEqual(record, None)
        in_file.close()
        log.debug('===== END TEST verify_parser RECOVERED =====')

    def test_verify_parser_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains metadata entries.
        This test includes testing metadata entries as well
        """
        log.debug('===== START TEST verify_parser RECOVERED =====')
        in_file = self.open_file(FILE)
        parser = self.create_rec_parser(in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE_REC, RESOURCE_PATH)

        self.assertEqual(self.rec_exception_callback_value, None)

        in_file.close()
        log.debug('===== END TEST verify_parser RECOVERED =====')

    def test_verify_parser_failure(self):
        """
        Read telemetered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains invalid entries.
        This test includes testing invalid entries as well
        """
        log.debug('===== START TEST failure verify_parser =====')
        in_file = self.open_file(FILE_FAILURE)
        parser = self.create_tel_parser(in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        self.assertNotEqual(self.tel_exception_callback_value, None)

        in_file.close()
        log.debug('===== END TEST failure verify_parser =====')

    def test_verify_parser_failure_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        The test log file contains invalid entries.
        This test includes testing invalid entries as well
        """
        log.debug('===== START TEST failure verify_parser RECOVERED =====')
        in_file = self.open_file(FILE_FAILURE)
        parser = self.create_rec_parser(in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE_REC, RESOURCE_PATH)

        self.assertNotEqual(self.rec_exception_callback_value, None)

        in_file.close()
        log.debug('===== END TEST failure verify_parser RECOVERED =====')
