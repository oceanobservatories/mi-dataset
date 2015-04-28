#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_zplsc_c_dcl
@file mi-dataset/mi/dataset/parser/test/test_zplsc_c_dcl.py
@author Richard Han (Raytheon), Ronald Ronquillo (Raytheon)
@brief Test code for a zplsc_c_dcl data parser
"""

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.dataset.parser.zplsc_c_dcl import ZplscCDclParser
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


from mi.idk.config import Config
RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver',
                             'zplsc_c', 'dcl', 'resource')

MODULE_NAME = 'mi.dataset.parser.zplsc_c_dcl'
CLASS_NAME = 'ZplscCInstrumentDataParticle'
PARTICLE_TYPE = 'zplsc_c_instrument'


@attr('UNIT', group='mi')
class ZplscCDclParserUnitTestCase(ParserUnitTestCase):
    """
    Zplsc_c_dcl Parser unit test suite
    """

    def create_zplsc_c_dcl_parser(self, file_handle):
        """
        This function creates a ZplscCDCL parser for recovered data.
        """
        return ZplscCDclParser(self.config, file_handle, self.rec_exception_callback)

    def open_file(self, filename):
        log.info('resource path = %s, file name = %s', RESOURCE_PATH, filename)
        return open(os.path.join(RESOURCE_PATH, filename), mode='r')

    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.exception_callback_value.append(exception)
        self.exceptions_detected += 1

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
        }

        self.exception_callback_value = []
        self.exceptions_detected = 0

    def test_zplsc_c_dcl_parser(self):
        """
        Test Zplsc C DCL parser
        Just test that it is able to parse the file and records are generated.
        """
        log.debug('===== START TEST ZPLSC_C_DCL Parser =====')

        in_file = self.open_file('20150406.zplsc.log')
        parser = self.create_zplsc_c_dcl_parser(in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(15)
        self.assertEqual(len(result), 1)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        in_file.close()

        log.debug('===== END TEST ZPLSC_C_DCL Parser  =====')

    def test_telem(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        in_file = self.open_file('20150407.zplsc.log')
        parser = self.create_zplsc_c_dcl_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(15)

        self.assertEqual(len(result), 15)
        self.assert_particles(result, '20150407.zplsc.yml', RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

    def test_variable_num_of_channels(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        The first for lines in the file exercise
        """
        in_file = self.open_file('20150407.zplsc_var_channels.log')
        parser = self.create_zplsc_c_dcl_parser(in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(15)

        self.assertEqual(len(result), 15)
        self.assert_particles(result, '20150407.zplsc_var_channels.yml', RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        See '20150407.zplsc_corrupt.log' file for line by line details of expected errors.
        """

        in_file = self.open_file('20150407.zplsc_corrupt.log')
        parser = parser = self.create_zplsc_c_dcl_parser(in_file)

        result = parser.get_records(100)
        log.error("RESULT: %s", [test.__dict__ for test in result])

        self.assertEqual(len(result), 1)
        self.assertEqual(len(self.exception_callback_value), 6)

        for i in range(len(self.exception_callback_value)):
            log.info('Exception: %s', self.exception_callback_value[i])

        in_file.close()

        log.debug('===== END TEST ZPLSC_C_DCL Parser  =====')

    def create_large_yml(self):
        """
        Create a large yml file corresponding to an actual recovered dataset.
        This is not an actual test - it allows us to create what we need
        for integration testing, i.e. a yml file.
        """

        in_file = self.open_file('20150407.zplsc_var_channels.log')
        parser = self.create_zplsc_c_dcl_parser(in_file)

        date, name, ext = in_file.name.split('.')
        date = date.split('/')[-1]
        out_file = '.'.join([date, name, 'yml'])
        log.info(out_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(1000)

        self.particle_to_yml(result, out_file)

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests.
        Since the same particles will be used for the driver test it is helpful to
        write them to .yml in the same form they need in the results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)
        fid.write('header:\n')
        fid.write("    particle_object: %s\n" % CLASS_NAME)
        fid.write("    particle_type: %s\n" % PARTICLE_TYPE)
        fid.write('data:\n')
        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            fid.write('  - _index: %d\n' % (i+1))
            # fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            # fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))
            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %2.1f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()
