#!/usr/bin/env python
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.logging import config    
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.core.versioning import version

log = get_logger()

__author__ = 'Joe Padula'


@version("0.0.2")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    
    config.add_configuration(os.path.join(basePythonCodePath, 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_mmp_cds',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaAbcdjmMmpCdsParserDataParticle'
    }
    
    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()
            
    with open(sourceFilePath, 'rb') as stream_handle:
        parser = MmpCdsParser(parser_config, stream_handle, exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()    
        
    return particleDataHdlrObj
