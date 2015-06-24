#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdmo_ghqr.imodem
@file mi-dataset/mi/dataset/driver/ctdmo_ghqr/imodem/ctdmo_ghqr_imodem_telemetered_driver.py
@author Mark Worden
@brief Driver for the ctdmo_ghqr_imodem instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdmo_ghqr_imodem import CtdmoGhqrImodemParser, \
    CtdmoGhqrImodemParticleClassKey, \
    CtdmoGhqrImodemMetadataTelemeteredDataParticle, \
    CtdmoGhqrImodemInstrumentTelemeteredDataParticle
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

        driver = CtdmoGhqrImodemTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdmoGhqrImodemTelemeteredDriver(SimpleDatasetDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_imodem',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                CtdmoGhqrImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                    CtdmoGhqrImodemMetadataTelemeteredDataParticle,
                CtdmoGhqrImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    CtdmoGhqrImodemInstrumentTelemeteredDataParticle,
            }
        }

        parser = CtdmoGhqrImodemParser(parser_config,
                                       stream_handle,
                                       self._exception_callback)

        return parser
