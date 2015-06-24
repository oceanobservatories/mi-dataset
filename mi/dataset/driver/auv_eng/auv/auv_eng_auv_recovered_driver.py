#!/usr/bin/env python

"""
@package mi.dataset.driver.auv_eng.auv
@file mi/dataset/driver/auv_eng/auv/auv_eng_auv_recovered_driver.py
@author Jeff Roy
@brief Driver for the auv_eng_auv instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.auv_eng_auv import AuvEngAuvParser
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

    with open(sourceFilePath, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = AuvEngAuvRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class AuvEngAuvRecoveredDriver(SimpleDatasetDriver):
    """
    Derived auv_eng_auv driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = AuvEngAuvParser(stream_handle,
                                 self._exception_callback,
                                 is_telemetered=False)

        return parser


