#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdmo_ghqr.sio.ctdmo_ghqr_sio_telemetered_driver
@file mi-dataset/mi/dataset/driver/ctdmo_ghqr/sio/ctdmo_ghqr_sio_telemetered_driver.py
@author Emily Hahn
@brief Driver for the ctdmo_ghqr_sio instrument
"""
from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.ctdmo_ghqr_sio import CtdmoGhqrSioTelemeteredParser
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

    log = get_logger()

    with open(sourceFilePath, 'rb') as stream_handle:

        def exception_callback(exception):
                log.debug("Exception: %s", exception)
                particleDataHdlrObj.setParticleDataCaptureFailure()

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['CtdmoGhqrSioTelemeteredInstrumentDataParticle',
                                                     'CtdmoGhqrSioTelemeteredOffsetDataParticle']
        }

        parser = CtdmoGhqrSioTelemeteredParser(parser_config, stream_handle, exception_callback)

        # create and instance of the concrete driver class defined below
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj
