# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Joe Padula"

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.dosta_abcdjm_dcl import DostaAbcdjmDclTelemeteredParser
from mi.core.versioning import version


@version("0.0.1")
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
        driver = DostaAbcdjmDclRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class DostaAbcdjmDclRecoveredDriver(SimpleDatasetDriver):
    """
    Derived ctdbp_p_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = DostaAbcdjmDclTelemeteredParser(stream_handle,
                                               self._exception_callback)

        return parser