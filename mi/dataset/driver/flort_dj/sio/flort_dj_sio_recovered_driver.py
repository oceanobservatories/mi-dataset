#!/usr/bin/env python

"""
@package mi.dataset.driver.flort_dj.sio
@file mi-dataset/mi/dataset/driver/flort_dj/sio/flort_dj_sio_recovered_driver.py
@author Joe Padula
@brief Recovered driver for the flort_dj_sio instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flort_dj_sio import FlortDjSioParser
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
        driver = FlortDjSioRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class FlortDjSioRecoveredDriver(SimpleDatasetDriver):
    """
    The flort_dj_sio recovered driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flort_dj_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortdRecoveredParserDataParticle'
        }

        parser = FlortDjSioParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)

        return parser

