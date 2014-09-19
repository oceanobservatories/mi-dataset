# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "mworden"

import os

from mi.core.log import get_logger
from mi.logging import config

from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcInstrumentDataParticle, \
    Pco2wAbcInstrumentBlankDataParticle, Pco2wAbcPowerDataParticle, Pco2wAbcMetadataDataParticle

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.pco2w_abc import Pco2wAbcParser


class Pco2wAbcDriver:
    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        with open(self._sourceFilePath, "r") as file_handle:
            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    exception_callback,
                                    None,
                                    None)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()

        return self._particleDataHdlrObj


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'instrument_particle_class': Pco2wAbcInstrumentDataParticle,
            'instrument_blank_particle_class': Pco2wAbcInstrumentBlankDataParticle,
            'power_particle_class': Pco2wAbcPowerDataParticle,
            'metadata_particle_class': Pco2wAbcMetadataDataParticle,
        }
    }

    driver = Pco2wAbcDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
