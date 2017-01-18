#!/usr/bin/env python

"""
@package mi.dataset.driver.flobn_cm_subcon
@file marine-integrations/mi/dataset/driver/ppsdn/flobn_m_subcon_recovered_driver.py
@author Rachel Manoni
@brief Driver for the flobn_cm_subcon instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flobn_cm_subcon import FlobnMSubconTemperatureParser
from mi.core.versioning import version


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
        driver = FlobnMSubconTemperatureRecoveredDriver(unused, stream_handle, particleDataHdlrObj)
        driver.processFileStream()
    return particleDataHdlrObj


class FlobnMSubconTemperatureRecoveredDriver(SimpleDatasetDriver):
    """
    Derived driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flobn_cm_subcon',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = FlobnMSubconTemperatureParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser

