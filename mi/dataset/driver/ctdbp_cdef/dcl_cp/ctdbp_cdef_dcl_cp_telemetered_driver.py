#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_cdef.dcl_cp.ctdbp_cdef_dcl_cp
@file mi-dataset/mi/dataset/driver/ctdbp_cdef/dcl_cp/ctdbp_cdef_dcl_cp.py
@author Tapana Gupta
@brief Driver for the ctdbp_cdef_dcl_cp instrument (Telemetered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdbp_cdef_dcl_cp import \
    CtdbpCdefDclCpParser, \
    CtdbpCdefDclCpTelemeteredParserDataParticle
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl_cp'

@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = CtdbpCdefDclCpTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdbpCdefDclCpTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived ctdbp_cdef_dcl_cp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: CtdbpCdefDclCpTelemeteredParserDataParticle

        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = CtdbpCdefDclCpParser(parser_config,
                                      stream_handle,
                                      self._exception_callback)

        return parser