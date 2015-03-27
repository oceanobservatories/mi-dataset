#!/usr/bin/env python

"""
@package mi.dataset.driver.winch_cspp
@file mi-dataset/mi/dataset/driver/winch_cspp_/winch_cspp_driver.py
@author Richard Han
@brief Driver for the  Winch CSPP platform

Release notes:

Initial Release
"""

from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.winch_cspp import WinchCsppParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = \
            WinchCsppDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class WinchCsppDriver(SimpleDatasetDriver):
    """
    Derived WinchCspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.winch_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'WinchCsppDataParticle'
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = WinchCsppParser(parser_config,
                                 stream_handle,
                                 self._exception_callback)

        return parser
