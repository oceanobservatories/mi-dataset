#!/usr/bin/env python
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Joe Padula'

import os

from mi.logging import config    
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.dosta_abcdjm_mmp_cds import DostaAbcdjmMmpCdsParser
from mi.core.versioning import version


@version("0.0.1")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_mmp_cds',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaAbcdjmMmpCdsParserDataParticle'
    }
    
    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()
            
    with open(sourceFilePath, 'rb') as stream_handle:
        parser = DostaAbcdjmMmpCdsParser(parser_config,
                                         None,
                                         stream_handle,
                                         lambda state, ingested: None,
                                         lambda data: None,
                                         exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()    
        
    return particleDataHdlrObj
