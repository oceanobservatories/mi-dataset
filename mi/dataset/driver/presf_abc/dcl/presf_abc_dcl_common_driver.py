#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Jeff Roy'


from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.presf_abc_dcl import\
    PresfAbcDclRecoveredTideDataParticle, \
    PresfAbcDclTelemeteredTideDataParticle, \
    PresfAbcDclRecoveredWaveDataParticle, \
    PresfAbcDclTelemeteredWaveDataParticle, \
    DataTypeKey, \
    TIDE_PARTICLE_CLASS_KEY, \
    WAVE_PARTICLE_CLASS_KEY, \
    PresfAbcDclParser

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

MODULE_NAME = 'mi.dataset.parser.presf_abc_dcl'

parser_config = {
    DataTypeKey.PRESF_ABC_DCL_TELEMETERED: {
        DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            TIDE_PARTICLE_CLASS_KEY: PresfAbcDclTelemeteredTideDataParticle,
            WAVE_PARTICLE_CLASS_KEY: PresfAbcDclTelemeteredWaveDataParticle,
        }
    },
    DataTypeKey.PRESF_ABC_DCL_RECOVERED: {
        DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            TIDE_PARTICLE_CLASS_KEY: PresfAbcDclRecoveredTideDataParticle,
            WAVE_PARTICLE_CLASS_KEY: PresfAbcDclRecoveredWaveDataParticle,
        }
    },
}


class PresfAbcDclDriver:

    def __init__ (self, sourceFilePath, particleDataHdlrObj, data_type_key):
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config
        self._data_type_key = data_type_key

    def process(self):

        def exception_callback(exception):
            log.debug("ERROR: " + exception)
            self._particleDataHdlrObj.setParticleDataCaptureFailure()

        with open(self._sourceFilePath, 'r') as stream_handle:
            parser = PresfAbcDclParser(parser_config.get(self._data_type_key),
                                       None, stream_handle,
                                       lambda state, ingested: None, lambda data: None,
                                       exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)
            driver.processFileStream()

        return self._particleDataHdlrObj

