##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'kustert'

import sys
import os

from mi.logging import config
from mi.core.log import get_logger
from mi.dataset.parser.parad_k_stc_imodem import Parad_k_stc_imodemRecoveredParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
from mi.core.versioning import version

@version("0.0.3")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    
    log = get_logger()
    
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_k_stc_imodem',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'Parad_k_stc_imodemRecoveredDataParticle'
    }
    
    
    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()
    
    with open(sourceFilePath, 'rb') as stream_handle:
        parser = Parad_k_stc_imodemRecoveredParser(parser_config, 
                                                   None, 
                                                   stream_handle,
                                                   lambda state, ingested : None, 
                                                   lambda data : None,
                                                   exception_callback)
    
        driver = DataSetDriver(parser, particleDataHdlrObj)
    
        driver.processFileStream()

    return particleDataHdlrObj
