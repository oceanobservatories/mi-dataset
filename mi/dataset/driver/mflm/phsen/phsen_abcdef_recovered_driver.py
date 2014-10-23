#!/usr/bin/env python

"""
@package mi.dataset.driver.mflm.phsen.phsen_abcdef_recovered_driver
@file mi-dataset/mi/dataset/driver/mflm/phsen/phsen_abcdef_recovered_driver.py
@author Ronald Ronquillo
@brief Driver for the phsen_abcdef instrument

Release notes:

Initial Release
"""

import os

from mi.logging import config
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.phsen_abcdef import PhsenRecoveredParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef',
        DataSetDriverConfigKeys.PARTICLE_CLASS: ['PhsenRecoveredMetadataDataParticle',
                                                 'PhsenRecoveredInstrumentDataParticle']
    }

    def exception_callback(exception):
        log.debug("ERROR: " + exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()
    
    with open(sourceFilePath, 'rb') as stream_handle:
        parser = PhsenRecoveredParser(parser_config,
                                      None,
                                      stream_handle,
                                      lambda state, ingested: None,
                                      lambda data: None,
                                      exception_callback)
        
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()    

        
    return particleDataHdlrObj