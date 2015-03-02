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
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()
    
    with open(sourceFilePath, 'rb') as file_handle:
        parser = AdcpPd0Parser(config, file_handle, exception_callback)
        
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream() 
  
    return particleDataHdlrObj
