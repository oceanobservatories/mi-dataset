#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file mi-dataset/mi/dataset/parser/test/test_cspp_eng_dcl.py
@author Jeff Roy
@brief Test code for a cspp_eng_dcl data parser

"""

import os

from nose.plugins.attrib import attr

from mi.idk.config import Config

from mi.core.log import get_logger

from mi.dataset.test.test_parser import ParserUnitTestCase

from mi.dataset.parser.cspp_eng_dcl import CsppEngDclParser
log = get_logger()

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                             'dataset', 'driver', 'cspp_eng',
                             'dcl', 'resource')

MODULE_NAME = 'mi.dataset.parser.cspp_eng_dcl'


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class CsppEngDclParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_dcl Parser unit test suite
    """

    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.info('START TEST SIMPLE')
        # test along the telemetered path, current config
        with open(os.path.join(RESOURCE_PATH, '20151010_002806.ucspp.log'), 'rU') as file_handle:
            parser = CsppEngDclParser({},
                                      file_handle,
                                      self.exception_callback)

            particles = parser.get_records(10)

            raise NameError


