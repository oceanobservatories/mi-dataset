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

from mi.dataset.parser.pco_a_2a_dcl import Pco2aDclParser, MODULE_NAME, \
    RECOVERED_AIR_PARTICLE_CLASS, TELEMETERED_AIR_PARTICLE_CLASS, \
    RECOVERED_WATER_PARTICLE_CLASS, TELEMETERED_WATER_PARTICLE_CLASS

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH
RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'pco_2a', 'dcl', 'resource')

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

    def create_parser(self, particle_class, file_handle):
        """
        This function creates a MetbkADcl parser for recovered data.
        """
        parser = Pco2aDclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            file_handle,
            lambda state, ingested: None,
            self.publish_callback,
            self.exception_callback)
        return parser

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def test_verify_record(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are there.
        """
        log.debug('===== START TEST verify record parser =====')
        in_file = self.open_file(FILE)
        parser = self.create_parser(TELEMETERED_AIR_PARTICLE_CLASS, in_file)

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
        parser = self.create_parser(TELEMETERED_AIR_PARTICLE_CLASS, in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()
        log.debug('===== END TEST verify parser =====')

    def test_verify_record_recovered(self):
        """
        Read recovered data from a file and pull out data particles
        one at a time. Verify that the results are there.
        """
        log.debug('===== START TEST verify_parser RECOVERED =====')
        in_file = self.open_file(FILE)
        parser = self.create_parser(RECOVERED_AIR_PARTICLE_CLASS, in_file)

        parser.get_records(RECORDS)

        self.assertListEqual(self.exception_callback_value, [])

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
        parser = self.create_parser(RECOVERED_AIR_PARTICLE_CLASS, in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE_REC, RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])

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
        parser = self.create_parser(TELEMETERED_AIR_PARTICLE_CLASS, in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        self.assertEqual(len(self.exception_callback_value), 24)

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
        parser = self.create_parser(RECOVERED_AIR_PARTICLE_CLASS, in_file)

        result = parser.get_records(RECORDS)
        self.assert_particles(result, YAML_FILE_REC, RESOURCE_PATH)

        self.assertEqual(len(self.exception_callback_value), 24)

        in_file.close()
        log.debug('===== END TEST failure verify_parser RECOVERED =====')
