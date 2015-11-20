#!/usr/bin/env python

"""
@package mi.dataset.driver.camhd_a
@file mi-dataset/mi/dataset/driver/camhd_a/camhd_a_telemetered_driver.py
@author Ronald Ronquillo
@brief Telemetered driver for the camhd_a instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.camhd_a import CamhdAParser
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

    with open(sourceFilePath, 'rb') as stream_handle:

        CamhdATelemeteredDriver(basePythonCodePath, stream_handle,
                                particleDataHdlrObj).processFileStream()

    return particleDataHdlrObj


class CamhdATelemeteredDriver(SimpleDatasetDriver):
    """
    The camhd_a driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, basePythonCodePath, stream_handle, particleDataHdlrObj):

        super(CamhdATelemeteredDriver, self).__init__(basePythonCodePath, stream_handle, particleDataHdlrObj)

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.camhd_a',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CamhdAInstrumentDataParticle'}

        parser = CamhdAParser(parser_config,
                              stream_handle,
                              self._exception_callback)

        return parser