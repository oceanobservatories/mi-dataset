#!/usr/bin/env python

"""
@package mi.dataset.driver.sio_eng/sio
@file mi/dataset/driver/sio_eng/sio/sio_eng_sio_telemetered_driver.py
@author Jeff Roy
@brief Driver for the sio_eng_sio instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.sio_eng_sio import SioEngSioParser
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

        # create and instance of the concrete driver class defined below
        driver = SioEngSioTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class SioEngSioTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived sio_eng_sio driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.sio_eng_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'SioEngSioTelemeteredDataParticle'
        }

        parser = SioEngSioParser(parser_config, stream_handle,
                                 self._exception_callback)

        return parser


