#!/usr/local/bin/python2.7
# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
# #

from mi.core.log import get_logger

from mi.dataset.parser.cg_cpm_eng_cpm import CgCpmEngCpmParser
from mi.dataset.dataset_driver import DataSetDriver

class CgCpmEngCpmDriver:

    def __init__ (self, sourceFilePath, particleDataHdlrObj, parser_config):
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config
    
    def process(self):
        
        log = get_logger()
        
        def exception_callback(exception):
            log.debug("ERROR: " + exception)
            self._particleDataHdlrObj.setParticleDataCaptureFailure()

        with open(self._sourceFilePath, 'r') as stream_handle:
            parser = CgCpmEngCpmParser(self._parser_config, stream_handle,
                                       exception_callback)
            
            driver = DataSetDriver(parser, self._particleDataHdlrObj)
            driver.processFileStream()    
        
        return self._particleDataHdlrObj
  
