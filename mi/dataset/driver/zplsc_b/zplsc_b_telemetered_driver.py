#!/usr/bin/env python

"""
@package mi.dataset.driver.zplsc_b
@file mi-dataset/mi/dataset/driver/zplsc_b/zplsc_b_telemetered_driver.py
@author Ronald Ronquillo
@brief Recovered driver for the zplsc_b instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.zplsc_b import ZplscBParser
from mi.core.versioning import version


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, outputFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param outputFilePath This is the full path of the file to be output
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        ZplscBTelemeteredDriver(basePythonCodePath, stream_handle, outputFilePath,
                                particleDataHdlrObj).processFileStream()

    return particleDataHdlrObj


class ZplscBTelemeteredDriver(SimpleDatasetDriver):
    """
    The zplsc_b driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, basePythonCodePath, stream_handle, outputFilePath, particleDataHdlrObj):

        self.outputFilePath = outputFilePath

        super(ZplscBTelemeteredDriver, self).__init__(basePythonCodePath, stream_handle, particleDataHdlrObj)

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.zplsc_b',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'ZplscBInstrumentDataParticle'}

        parser = ZplscBParser(parser_config,
                              stream_handle,
                              self._exception_callback,
                              self.outputFilePath)

        return parser