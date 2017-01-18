#!/usr/bin/env python

"""
@package mi.dataset.driver.adcpt_m
@file marine-integrations/mi/dataset/driver/adcpt_m_/ce/adcpt_m_log9_recovered_driver.py
@author Tapana Gupta
@brief Driver for the adcpt_m instrument (Log9 data file)

Release notes:

Initial Release
"""

from mi.core.versioning import version
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcpt_m_log9 import AdcptMLog9Parser


@version("15.6.1")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param unused
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'r') as stream_handle:
        # create an instance of the concrete driver class defined below
        driver = AdcptMLog9RecoveredDriver(unused, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class AdcptMLog9RecoveredDriver(SimpleDatasetDriver):
    """
    Derived adcpt_m_log9 driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_m_log9',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptMLog9InstrumentDataParticle'
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = AdcptMLog9Parser(parser_config,
                                  stream_handle,
                                  self._exception_callback)

        return parser
