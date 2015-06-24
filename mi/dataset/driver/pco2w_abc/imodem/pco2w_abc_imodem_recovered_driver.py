#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2w_abc.imodem
@file mi-dataset/mi/dataset/driver/pco2w_abc/imodem/pco2w_abc_imodem_recovered_driver.py
@author Mark Worden
@brief Driver for the pco2w_abc_imodem instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.pco2w_abc_imodem import Pco2wAbcImodemParser
from mi.dataset.parser.pco2w_abc_particles import \
    Pco2wAbcParticleClassKey, \
    Pco2wAbcImodemInstrumentBlankRecoveredDataParticle, \
    Pco2wAbcImodemInstrumentRecoveredDataParticle, \
    Pco2wAbcImodemPowerRecoveredDataParticle, \
    Pco2wAbcImodemControlRecoveredDataParticle, \
    Pco2wAbcImodemMetadataRecoveredDataParticle
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

        driver = Pco2wAbcImodemRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class Pco2wAbcImodemRecoveredDriver(SimpleDatasetDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS:
                Pco2wAbcImodemMetadataRecoveredDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS:
                Pco2wAbcImodemPowerRecoveredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                Pco2wAbcImodemInstrumentRecoveredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                Pco2wAbcImodemInstrumentBlankRecoveredDataParticle,
                Pco2wAbcParticleClassKey.CONTROL_PARTICLE_CLASS:
                Pco2wAbcImodemControlRecoveredDataParticle,
            }
        }

        parser = Pco2wAbcImodemParser(parser_config,
                                      stream_handle,
                                      self._exception_callback)

        return parser
