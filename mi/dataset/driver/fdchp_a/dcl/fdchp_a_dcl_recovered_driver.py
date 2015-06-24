#!/usr/bin/env python

"""
@package mi.dataset.driver.fdchp_a.dcl
@file mi/dataset/driver/fdchp_a/dcl/fdchp_a_dcl_recovered_driver.py
@author Emily Hahn
@brief Driver for the fdchp series a through dcl recovered instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.fdchp_a_dcl import FdchpADclParser
from mi.core.versioning import version


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'r') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = FdchpADclRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class FdchpADclRecoveredDriver(SimpleDatasetDriver):
    """
    Derived fdchp a dcl driver class
    All this needs to do is create a concrete _build_parser method
    """
    def _build_parser(self, stream_handle):
        # build the parser
        return FdchpADclParser(stream_handle, self._exception_callback, is_telemetered=False)


