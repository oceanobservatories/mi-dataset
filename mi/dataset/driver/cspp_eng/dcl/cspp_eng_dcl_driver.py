#!/usr/bin/env python

"""
@package mi.dataset.driver.cspp_eng.dcl
@file mi-dataset/mi/dataset/driver/cg_dcl_eng/dcl/cspp_eng_dcl_driver.py
@author Jeff Roy
@brief Driver for the cspp_eng_dcl instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.cspp_eng_dcl import CsppEngDclParser
from mi.core.versioning import version


@version("0.1.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rU') as stream_handle:

        driver = CsppEngDclDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CsppEngDclDriver(SimpleDatasetDriver):
    """
    Derived flntu_x_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = CsppEngDclParser({},
                                  stream_handle,
                                  self._exception_callback)

        return parser


