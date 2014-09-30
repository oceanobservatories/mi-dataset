#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2a_a.dcl.pco2a_a_dcl_driver
@file mi/dataset/driver/pco2a_a/dcl/pco2a_a_dcl_driver.py
@author Sung Ahn
@brief For creating a pco2a_a_dcl driver.

"""


from mi.core.log import get_logger
from mi.dataset.parser.pco2a_a_dcl import Pco2aADclParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

log = get_logger()

from mi.dataset.parser.pco2a_a_dcl import Pco2aADclParticleClassKey, \
    Pco2aADclTelemeteredInstrumentDataParticleAir, \
    Pco2aADclTelemeteredInstrumentDataParticleWater, \
    Pco2aADclRecoveredInstrumentDataParticleAir, \
    Pco2aADclRecoveredInstrumentDataParticleWater


MODULE_NAME = 'mi.dataset.parser.pco2a_a_dcl'

TELEMETERED_PARTICLE_CLASSES = {
    Pco2aADclParticleClassKey.AIR_PARTICLE_CLASS: Pco2aADclTelemeteredInstrumentDataParticleAir,
    Pco2aADclParticleClassKey.WATER_PARTICLE_CLASS: Pco2aADclTelemeteredInstrumentDataParticleWater
}

RECOVERED_PARTICLE_CLASSES = {
    Pco2aADclParticleClassKey.AIR_PARTICLE_CLASS: Pco2aADclRecoveredInstrumentDataParticleAir,
    Pco2aADclParticleClassKey.WATER_PARTICLE_CLASS: Pco2aADclRecoveredInstrumentDataParticleWater
}


def process(source_file_path, particle_data_hdlr_obj, particle_classes):

    with open(source_file_path, "r") as stream_handle:
        parser = Pco2aADclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: None,
             DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: particle_classes},
            stream_handle,
            lambda state, ingested: None,
            lambda data: log.trace("Found data: %s", data),
            lambda ex: particle_data_hdlr_obj.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_hdlr_obj)
        driver.processFileStream()