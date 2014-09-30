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


MODULE_NAME = 'mi.dataset.parser.pco2a_a_dcl'
RECOVERED_PARTICLE_CLASS = 'Pco2aADclRecoveredInstrumentDataParticle'
TELEMETERED_PARTICLE_CLASS = 'Pco2aADclTelemeteredInstrumentDataParticle'


def process(source_file_path, particle_data_hdlr_obj, particle_class):

    with open(source_file_path, "r") as stream_handle:
        parser = Pco2aADclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            stream_handle,
            lambda state, ingested: None,
            lambda data: log.trace("Found data: %s", data),
            lambda ex: particle_data_hdlr_obj.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_hdlr_obj)
        driver.processFileStream()