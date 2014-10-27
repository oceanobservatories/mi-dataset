#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_ctdbp_cdef_cp.py
@author Tapana Gupta
@brief Test code for ctdbp_cdef_cp data parser

Files used for testing:

data1.log
  Contains Header + 100 Sensor records

invalid_data.log
  Contains 7 lines of invalid data

no_sensor_data.log
  Contains a header section and no sensor records

"""


import unittest
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_cdef_cp import CtdbpCdefCpParser

from mi.dataset.parser.ctdbp_cdef_cp import  DATA_MATCHER

from mi.core.exceptions import SampleException

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH

RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'ctdbp_cdef', 'cp', 'resource')

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_cp'

SIMPLE_LOG_FILE = "simple_test.log"
RAW_INPUT_DATA_1 = "raw_input1.log"
EXTRACTED_DATA_FILE = "extracted_data.log"
INVALID_DATA_FILE = "invalid_data.log"
NO_SENSOR_DATA_FILE = "no_sensor_data.log"
DATA_FILE_1 = "data1.log"

# Define number of expected records/exceptions for various tests
NUM_REC_SIMPLE_LOG_FILE = 5
NUM_REC_DATA_FILE1 = 100
NUM_INVALID_EXCEPTIONS = 11

YAML_FILE = "data1.yml"


@attr('UNIT', group='mi')
class CtdbpCdefCpParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_cp Parser unit test suite
    """

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def open_file_write(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='w')
        return file

    def create_rec_parser(self, file_handle, new_state=None):
        """
        This function creates a CtdbpCdefCp parser for recovered data.
        """
        parser = CtdbpCdefCpParser(self.rec_config,
                                   file_handle,
                                   lambda state, ingested: None,
                                   self.publish_callback,
                                   self.exception_callback)
        return parser

    def test_verify_record(self):
        """
        Simple test to verify that records are successfully read and parsed from a data file
        """
        log.debug('===== START SIMPLE TEST =====')
        in_file = self.open_file(SIMPLE_LOG_FILE)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_SIMPLE_LOG_FILE
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END SIMPLE TEST =====')

    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(DATA_FILE_1)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_DATA_FILE1
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END YAML TEST =====')


    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_DATA_FILE)
        parser = self.create_rec_parser(in_file)

        # Try to get records and verify that none are returned.
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(len(self.exception_callback_value), NUM_INVALID_EXCEPTIONS)

        in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')


    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        in_file = self.open_file(NO_SENSOR_DATA_FILE)
        parser = self.create_rec_parser(in_file)

        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)
        self.assertEqual(result, [])

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

        log.debug('===== END TEST NO SENSOR DATA =====')



    # This is not really a test. This is a little module to read in a log file, and extract fields from the data,
    # converting Hex values to Integers. It writes the converted data to a log file, in a format that can be easily
    # imported into a spreadsheet where the data is then converted to yaml format.
    # The reason for including this here is that data validation is also being performed.
    def extract_data_particle_from_log_file(self):
        in_file = self.open_file(RAW_INPUT_DATA_1)
        out_file = self.open_file_write(EXTRACTED_DATA_FILE)

        for line in in_file:

            match = DATA_MATCHER.match(line)
            if not match:
                raise SampleException("CtdParserDataParticle: No regex match of \
                                  parsed sample data: [%s]", line)
            try:
                # grab Hex values, convert to int
                temp = str(int(match.group('temp'), 16))
                cond = str(int(match.group('cond'), 16))
                press = str(int(match.group('press'), 16))
                press_temp = str(int(match.group('press_temp'), 16))
                ctd_time = str(int(match.group('ctd_time'), 16))

                outline = temp + '\t' + cond + '\t' + press + '\t' + press_temp + '\t' + ctd_time + '\n'

                out_file.write(outline)

            except (ValueError, TypeError, IndexError) as ex:
                raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, line))

        in_file.close()
        out_file.close()

        self.assertListEqual(self.exception_callback_value, [])
