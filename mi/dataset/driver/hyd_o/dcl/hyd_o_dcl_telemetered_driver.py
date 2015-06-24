"""
@package mi.dataset.driver.hyd_o.dcl
@file mi-dataset/mi/dataset/driver/hyd_o/dcl/hyd_o_dcl_telemetered_driver.py
@author Emily Hahn
@brief Telemetered driver for the hydrogen series o through a dcl instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.hyd_o_dcl import HydODclParser
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
        driver = HydODclTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class HydODclTelemeteredDriver(SimpleDatasetDriver):
    """
    The hyd_o_dcl driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):
        """
        Build the parser for the input stream handle, no parser config is passed in
        :param stream_handle: The stream handle of the file to parse
        :return: The instantiation of the HydODclParser class
        """
        return HydODclParser(stream_handle, self._exception_callback, is_telemetered=True)
