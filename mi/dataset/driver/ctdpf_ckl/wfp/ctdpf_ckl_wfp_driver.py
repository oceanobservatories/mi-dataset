#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os
import sys

from mi.logging import config
from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.ctdpf_ckl_wfp import CtdpfCklWfpParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

class CtdpfCklWfpDriver:

    def __init__(self, sourceFilePath, particleDataHdlrObj, config):
        
        try:
            if config is not None:
                pass
        except NameError:
                print "No defined config"
                sys.exit(1)

        try:
            if sourceFilePath is not None:
                pass
        except NameError:
            try:
                sourceFilePath = sys.argv[1]
            except IndexError:
                print "Need a source file path"
                sys.exit(1)

        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._config = config

    def process(self):

        log = get_logger()

        try:
            file_handle = open(self._sourceFilePath, 'rb')
            filesize = os.path.getsize(file_handle.name)
            state = None
            parser_state = None
            
            def state_callback(state, ingested):
                pass
        
            def pub_callback(data):
                log.trace("Found data: %s", data)
            
            def sample_exception_callback(exception):
                self._particleDataHdlrObj.setParticleDataCaptureFailure()
                    
            parser = CtdpfCklWfpParser(self._config, parser_state, file_handle, lambda state, 
                                       ingested: state_callback(state, ingested), pub_callback, 
                                       sample_exception_callback, filesize)
    
            driver = DataSetDriver(parser, self._particleDataHdlrObj)
        
            driver.processFileStream()  
        finally:
            file_handle.close()
        
        return self._particleDataHdlrObj

