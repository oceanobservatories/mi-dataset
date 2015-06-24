#!/usr/bin/env python

"""
@package mi.dataset.driver.vel3d_l.wfp.sio
@file marine-integrations/mi/dataset/driver/vel3d_l/wfp/sio/vel3d_l_wfp_sio_telemetered_driver.py
@author Tapana Gupta
@brief Driver for the vel3d_l_wfp_sio instrument

Release notes:

Initial Release
"""

from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.vel3d_l_wfp import Vel3dLWfpSioParser
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

    with open(sourceFilePath, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = Vel3dlWfpSioTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class Vel3dlWfpSioTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived vel3d_l_wfp_sio_telemetered driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['Vel3dLWfpInstrumentParticle',
                                                     'Vel3dLWfpSioMuleMetadataParticle']
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = Vel3dLWfpSioParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser

