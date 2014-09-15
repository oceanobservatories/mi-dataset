#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.parser.adcps_jln import AdcpsJlnParticle

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    from mi.logging import config
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    log = get_logger()

    config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcps_jln',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpsJlnParticle'
        }
    log.trace("My ADCPS JLN Config: %s", config)

    def exception_callback(exception):
        log.debug("ERROR: " + exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()
    
    with open(sourceFilePath, 'rb') as file_handle:
        parser = AdcpPd0Parser(config, None, file_handle,
            lambda state, ingested: None,
            lambda data: None, exception_callback)
        
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream() 
  
    return particleDataHdlrObj