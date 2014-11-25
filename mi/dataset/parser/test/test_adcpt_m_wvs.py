#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcpt_m_wvs
@fid marine-integrations/mi/dataset/parser/test/test_adcpt_mwvs.py
@author Ronald Ronquillo
@brief Test code for a Adcpt_M_WVS data parser
"""

from nose.plugins.attrib import attr
import os

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.adcpt_m_wvs import AdcptMWVSParser

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH
RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'adcpt_m', 'resource')


@attr('UNIT', group='mi')
class AdcptMWVSParserUnitTestCase(ParserUnitTestCase):
    """
    Adcpt_M_WVS Parser unit test suite
    """

    def create_parser(self, file_handle):
        """
        This function creates a AdcptMWVS parser for recovered data.
        """
        parser = AdcptMWVSParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_m_wvs',
             DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptMWVSInstrumentDataParticle'},
            file_handle,
            lambda state, ingested: None,
            self.publish_callback,
            self.exception_callback)
        return parser

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='rb')
        return file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same
        particles will be used for the driver test it is helpful to write them to .yml in the same
        form they need in the results.yml here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            log.warn("PRINT DICT: %s", particles[i].generate_dict())

            fid.write('  - _index: %d\n' % (i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), str):
                    fid.write('    %s: %r\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), float):
                    fid.write('    %s: %.8f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), list):
                    if isinstance(val.get('value')[0], float):
                        fid.write('    %s: [' % (val.get('value_id')))
                        fid.write(", ".join(map(lambda x: '{0:06f}'.format(x), val.get('value'))))
                        fid.write(']\n')
                    else:
                        fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """
        file_name = 'CE01ISSM-ADCPT_20140418_000_TS1404180021.WVS'

        fid = open(os.path.join(RESOURCE_PATH, file_name), 'rb')

        self.stream_handle = fid

        self.parser = self.create_parser(fid)

        particles = self.parser.get_records(5)

        self.particle_to_yml(particles, 'CE01ISSM-ADCPT_20140418_000_TS1404180021.yml')
        fid.close()

    def test_parse_input(self):
        """
        Read a file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done in the
        tests below. This is mainly for debugging the regexes.
        """
        in_file = self.open_file('CE01ISSM-ADCPT_20140418_000_TS1404180021.WVS')
        parser = self.create_parser(in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(515)
        self.assertEqual(len(result), 515)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

    def test_recov_excerpt(self):
        """
        Read a file and pull out a single data particle.
        Verify that the results are those we expected.
        """
        # CE01ISSM-ADCPT_20140418_000_TS1404180021 - excerpt.WVS contains only one record
        # used for a quick sanity test

        in_file = self.open_file('CE01ISSM-ADCPT_20140418_000_TS1404180021 - excerpt.WVS')
        parser = self.create_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(1)

        self.assertEqual(len(result), 1)
        self.assert_particles(result, 'CE01ISSM-ADCPT_20140418_000_TS1404180021 - excerpt.yml', RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

    def test_recovmod(self):
        """
        Test that the data type ID's required to be populated in a particle are filled with NULL
        when not present in a record.
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        in_file = self.open_file('CE01ISSM-ADCPT_20140418_000_TS1404180021 - mod.WVS')
        parser = self.create_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(1)

        self.assertEqual(len(result), 1)
        self.assert_particles(result, 'CE01ISSM-ADCPT_20140418_000_TS1404180021 - mod.yml', RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

    def test_recov(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        in_file = self.open_file('CE01ISSM-ADCPT_20140418_000_TS1404180021.WVS')
        parser = self.create_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(5)

        self.assertEqual(len(result), 5)
        self.assert_particles(result, 'CE01ISSM-ADCPT_20140418_000_TS1404180021.yml', RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

    def test_bad_data(self):
        """
        Ensure that the sieve is robust enough to skip records that are incomplete/missing bytes.
        """
        # CE01ISSM-ADCPT_20140418_000_TS1404180021 - corrupt.WVS is a copy of
        # CE01ISSM-ADCPT_20140418_000_TS1404180021.WVS with bytes arbitrarily removed in 10 records
        # including the last record in the binary file

        fid = open(os.path.join(RESOURCE_PATH, 'CE01ISSM-ADCPT_20140418_000_TS1404180021 - corrupt.WVS'), 'rb')

        parser = self.create_parser(fid)

        result = parser.get_records(515)
        self.assertEqual(len(result), 505)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
            log.debug('Exception: %s', self.exception_callback_value[i])

        fid.close()

    def test_missing_file_time(self):
        """
        Ensure a particle is created with missing file time and sequence filled with Null and
        the recoverable sample exception is recorded.
        """
        fid = open(os.path.join(RESOURCE_PATH, 'CE01ISSM-ADCPT_20140418_NoTime - excerpt.WVS'), 'rb')

        parser = self.create_parser(fid)

        result = parser.get_records(1)
        self.assertEqual(len(result), 1)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
            log.debug('Exception: %s', self.exception_callback_value[i])

        fid.close()