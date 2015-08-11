#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_p.dcl
@file mi-dataset/mi/dataset/driver/ctdbp_p/dcl/ctdbp_p_dcl_recovered_driver.py
@author Jeff Roy
@brief Driver for the ctdbp_p_dcl instrument (Recovered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_p_dcl import CtdbpPDclCommonParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.ctdbp_p_dcl'

CTDBP_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdbpPDclRecoveredDataParticle'
}

@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rU') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = CtdbpPDclRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdbpPDclRecoveredDriver(SimpleDatasetDriver):
    """
    Derived ctdbp_p_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = CtdbpPDclCommonParser(CTDBP_RECOV_CONFIG,
                                       stream_handle,
                                       self._exception_callback)

        return parser