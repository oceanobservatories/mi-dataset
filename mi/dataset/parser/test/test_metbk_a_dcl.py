#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_metbk_a_dcl
@file marine-integrations/mi/dataset/parser/test/test_metbk_a_dcl.py
@author Ronald Ronquillo
@brief Test code for a metbk_a_dcl data parser

In the following files, Metadata consists of 4 records.
There is 1 group of Sensor Data records for each set of metadata.

Files used for testing:

20140805.metbk2.log
  Metadata - 4 set,  Sensor Data - 1430 records

20140901.metbk2.log
  Metadata - 7 sets,  Sensor Data - 1061 records

20140902.metbk2.log
  Metadata - 0 sets,  Sensor Data - 863 records

20140917.metbk2.log
  Metadata - 0 sets,  Sensor Data - 904 records

"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.dcl_file_common import StateKey
from mi.dataset.parser.metbk_a_dcl import \
    MetbkADclRecoveredParser, \
    MetbkADclTelemeteredParser

from mi.idk.config import Config
from mi.idk.result_set import ResultSet

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver', 'metbk_a', 'dcl', 'resource')
MODULE_NAME = 'mi.dataset.parser.metbk_a_dcl'


FILE_4_1430 = '20140805.metbk2.log'
FILE_4_0 = '20140805.metbk2_bad_sensor.log'
FILE_7_0 = '20140901.metbk2_no_sensor.log'
FILE_7_1061 = '20140901.metbk2.log'
FILE_0_863 = '20140902.metbk2.log'
FILE_0_904 = '20140917.metbk2.log'

YML_0_863 = 'rec_20140902.metbk2.yml'
YML_4_1430 = 'tel_20140805.metbk2.yml'
YML_7_1061 = 'tel_20140901.metbk2.yml'
YML_0_904 = 'rec_20140917.metbk2.yml'


RECORDS_FILE_4_1430 = 1430      # number of records expected
RECORDS_FILE_7_1061 = 1061      # number of records expected
RECORDS_FILE_0_863 = 863      # number of records expected
RECORDS_FILE_0_904 = 904
EXCEPTIONS_FILE8 = 47    # number of exceptions expected


class IndexedResultSet(ResultSet):

    def verify(self, particles, index=1):
        """
        Verify particles passed in against result set read
        in the ctor.

        Ensure:
          - Verify particles as a set
          - Verify individual particle data

        store verification result in the object and
        return success or failure.
        @param particls: list of particles to verify.
        @return True if verification successful, False otherwise
        """
        self._clear_report()
        result = True

        if self._verify_set(particles):
            result = self._verify_particles(particles, index)
        else:
            result = False

        if not result:
            log.error("Failed verification: \n%s", self.report())

        return result

    def _verify_particles(self, particles, index=1):
        """
        Verify data in the particles individually.
        - Verify order based on _index
        - Verify parameter data values
        - Verify there are extra or missing parameters
        """
        result = True

        for particle in particles:
            particle_def = self._result_set_data.get(index)
            errors = []

            # No particle definition, we fail
            if particle_def is None:
                errors.append("no particle result defined for index %d" % index)

            # Otherwise lets do some validation
            else:
                errors += self._get_particle_header_errors(particle, particle_def)
                errors += self._get_particle_data_errors(particle, particle_def)

            if len(errors):
                self._add_to_report("Failed particle validation for index %d" % index)
                self._add_to_report(errors, 1)
                result = False

            index += 1

        return result


@attr('UNIT', group='mi')
class MetbkADclParserUnitTestCase(ParserUnitTestCase):
    """
    metbk_a_dcl Parser unit test suite
    """
    def create_rec_parser(self, file_handle, new_state=None):
        """
        This function creates a MetbkADcl parser for recovered data.
        """
        parser = MetbkADclRecoveredParser(self.rec_config,
                                          file_handle, new_state, self.rec_state_callback,
                                          self.rec_pub_callback, self.rec_exception_callback)
        return parser

    def create_tel_parser(self, file_handle, new_state=None):
        """
        This function creates a MetbkADcl parser for telemetered data.
        """
        parser = MetbkADclTelemeteredParser(self.tel_config,
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
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'MetbkADclRecoveredInstrumentDataParticle'
        }

        self.tel_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'MetbkADclTelemeteredInstrumentDataParticle'
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

    def test_big_giant_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done during
        integration and qualification testing.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        in_file = self.open_file(FILE_0_863)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = RECORDS_FILE_0_863
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertEqual(self.rec_exception_callback_value, None)

        log.debug('===== START TEST BIG GIANT INPUT TELEMETERED =====')
        in_file = self.open_file(FILE_0_904)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = RECORDS_FILE_0_904
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertEqual(self.tel_exception_callback_value, None)

        log.debug('===== END TEST BIG GIANT INPUT =====')
        
    def test_get_many(self):
        """
        Read a file and pull out multiple data particles at one time.
        Verify that the results are those we expected.
        """
        log.debug('===== START TEST GET MANY RECOVERED =====')
        in_file = self.open_file(FILE_0_863)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_0_863)

        # self.assertEqual(result, expected_particle)
        self.assert_particles(result, YML_0_863, RESOURCE_PATH)

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== START TEST GET MANY TELEMETERED =====')
        in_file = self.open_file(FILE_7_1061)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_7_1061)
        self.assert_particles(result, YML_7_1061, RESOURCE_PATH)

        self.assertEqual(self.tel_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST GET MANY =====')

    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE_4_0)
        parser = self.create_rec_parser(in_file)

        # Try to get records and verify that none are returned.
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(self.rec_exceptions_detected, EXCEPTIONS_FILE8)

        in_file.close()

        # log.debug('===== START TEST INVALID SENSOR DATA TELEMETERED =====')
        # in_file = self.open_file(FILE8)
        # parser = self.create_tel_parser(in_file)
        #
        # # Try to get records and verify that none are returned.
        # result = parser.get_records(1)
        # self.assertEqual(result, [])
        # self.assertEqual(self.tel_exceptions_detected, EXCEPTIONS_FILE8)
        #
        # in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')
        
    def test_mid_state_start(self):
        """
        Test starting a parser with a state in the middle of processing.
        """
        log.debug('===== START TEST MID-STATE START RECOVERED =====')

        in_file = self.open_file(FILE_0_904)
        rs_file = os.path.join(RESOURCE_PATH, YML_0_904)
        rs = IndexedResultSet(rs_file)

        # Start at the beginning of record 27 (of 904 total)
        initial_state = {
            StateKey.POSITION: 3042
        }

        parser = self.create_rec_parser(in_file, new_state=initial_state)

        # Generate a list of expected result particles.
        for count in xrange(1, 27):
            del rs._result_set_data[count]

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_0_904)

        # compare results particles and assert that the output was successful
        self.assertTrue(rs.verify(result, 27),
                        msg=('Failed unit test data validation for file %s' % YML_0_904))
        #
        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()
        
        # log.debug('===== START TEST MID-STATE START TELEMETERED =====')
        #
        # in_file = self.open_file(FILE2)
        #
        # # Start at the beginning of record 11 (of 30 total).
        # initial_state = {
        #     StateKey.POSITION: 1017
        # }
        #
        # parser = self.create_tel_parser(in_file, new_state=initial_state)
        #
        # # Generate a list of expected result particles.
        # expected_particle = []
        # for expected in EXPECTED_FILE2[-20: ]:
        #     particle = MetbkADclTelemeteredInstrumentDataParticle(expected)
        #     expected_particle.append(particle)
        #
        # # In a single read, get all particles for this file.
        # result = parser.get_records(len(expected_particle))
        # self.assertEqual(result, expected_particle)
        #
        # self.assertEqual(self.tel_exception_callback_value, None)
        # in_file.close()

        log.debug('===== END TEST MID-STATE START =====')

    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        in_file = self.open_file(FILE_7_0)
        parser = self.create_rec_parser(in_file)

        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)
        self.assertEqual(result, [])

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        # log.debug('===== START TEST NO SENSOR DATA TELEMETERED =====')
        # in_file = self.open_file(FILE1)
        # parser = self.create_tel_parser(in_file)
        #
        # # Try to get a record and verify that none are produced.
        # result = parser.get_records(1)
        # self.assertEqual(result, [])
        #
        # self.assertEqual(self.tel_exception_callback_value, None)
        # in_file.close()

        log.debug('===== END TEST NO SENSOR DATA =====')

    def test_set_state(self):
        """
        This test verifies that the state can be changed after starting.
        Some particles are read and then the parser state is modified to
        skip ahead or back.
        """
        log.debug('===== START TEST SET STATE RECOVERED =====')

        #Read and verify 5 particles (of the 904).
        in_file = self.open_file(FILE_0_904)
        rs_file = os.path.join(RESOURCE_PATH, YML_0_904)
        rsa = IndexedResultSet(rs_file)

        parser = self.create_rec_parser(in_file)

        # Generate a list of expected result particles.
        for count in xrange(6, 905):
            del rsa._result_set_data[count]

        result = parser.get_records(5)
        self.assertTrue(rsa.verify(result),
                        msg=('Failed unit test data validation for file %s' % YML_0_904))
        self.assertEqual(self.rec_exception_callback_value, None)

        # Skip ahead in the file so that we get the last 10 particles.
        new_state = {
            StateKey.POSITION: 104598
        }

        # Set the state.
        parser.set_state(new_state)

        # Read and verify the last 10 particles.
        rsb = IndexedResultSet(rs_file)
        for count in xrange(1, 895):
            del rsb._result_set_data[count]

        result = parser.get_records(10)
        self.assertTrue(rsb.verify(result, 895),
                        msg=('Failed unit test data validation for file %s' % YML_0_904))
        self.assertEqual(self.rec_exception_callback_value, None)

        in_file.close()


        # log.debug('===== START TEST SET STATE TELEMETERED =====')
        #
        # in_file = self.open_file(FILE5)
        # parser = self.create_tel_parser(in_file)
        #
        # # Read and verify 20 particles (of the 24).
        # for expected in EXPECTED_FILE5[ : 20]:
        #
        #     # Generate expected particle
        #     expected_particle = MetbkADclTelemeteredInstrumentDataParticle(expected)
        #
        #     # Get record and verify.
        #     result = parser.get_records(1)
        #     self.assertEqual(result, [expected_particle])
        #
        # # Skip back in the file so that we get the last 17 particles.
        # new_state = {
        #     StateKey.POSITION: 992,
        # }
        #
        # # Set the state.
        # parser.set_state(new_state)
        #
        # # Read and verify the last 17 particles.
        # for expected in EXPECTED_FILE5[-17: ]:
        #
        #     # Generate expected particle
        #     expected_particle = MetbkADclTelemeteredInstrumentDataParticle(expected)
        #
        #     # Get record and verify.
        #     result = parser.get_records(1)
        #     self.assertEqual(result, [expected_particle])

        log.debug('===== END TEST SET STATE =====')

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE RECOVERED =====')
        in_file = self.open_file(FILE_4_1430)
        parser = self.create_tel_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(RECORDS_FILE_4_1430)
        self.assert_particles(result, YML_4_1430, RESOURCE_PATH)

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        # log.debug('===== START TEST SIMPLE TELEMETERED =====')
        # in_file = self.open_file(FILE3)
        # parser = self.create_tel_parser(in_file)
        #
        # for expected in EXPECTED_FILE3:
        #
        #     # Generate expected particle
        #     expected_particle = MetbkADclTelemeteredInstrumentDataParticle(expected)
        #
        #     # Get record and verify.
        #     result = parser.get_records(1)
        #     self.assertEqual(result, [expected_particle])
        #
        # self.assertEqual(self.tel_exception_callback_value, None)
        # in_file.close()

        log.debug('===== END TEST SIMPLE =====')
