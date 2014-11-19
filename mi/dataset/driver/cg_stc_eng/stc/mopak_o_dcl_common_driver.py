#!/usr/local/bin/python2.7
# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
# #

from mi.core.log import get_logger

from mi.dataset.parser.mopak_o_dcl import MopakODclParser
from mi.dataset.dataset_driver import DataSetDriver

class MopakDriver:
    
    def __init__ (self, sourceFilePath, particleDataHdlrObj, parser_config):

       self._sourceFilePath = sourceFilePath
       self._particleDataHdlrObj = particleDataHdlrObj
       self._parser_config = parser_config
    
    def process(self):
        
        log = get_logger()
        
        def exception_callback(exception):
            log.debug("ERROR: %r", exception)
            self._particleDataHdlrObj.setParticleDataCaptureFailure()

        pathList = (self._sourceFilePath.split('/'))
        filename = pathList[len(pathList) - 1]

        with open(self._sourceFilePath, 'rb') as stream_handle:
            parser = MopakODclParser(self._parser_config, None, stream_handle,
                                        filename, lambda state, ingested: None,
                                        lambda data: None, exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)
            driver.processFileStream()    
        
        return self._particleDataHdlrObj
  
