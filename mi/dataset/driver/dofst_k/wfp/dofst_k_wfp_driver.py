#!/usr/local/bin/python2.7
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
from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser

class DofstKWfpDriver:

    def __init__(self, basePythonCodePath, sourceFilePath, particleDataHdlrObj, config):
        
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
                
        self._basePythonCodePath = basePythonCodePath
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._config = config
        
    def process(self):
    
        log = get_logger()
        stream_handle = open(self._sourceFilePath, 'rb')
        filesize = os.path.getsize(stream_handle.name)
        
        def state_callback(state, ingested):
            pass

        def pub_callback(data):
            log.trace("Found data: %s", data)
        
        def exp_callback(exception):
            self._particleDataHdlrObj.setParticleDataCaptureFailure()
        
        try:
            parser = DofstKWfpParser(self._config, None, stream_handle, state_callback, pub_callback, exp_callback, filesize)
            driver = DataSetDriver(parser, self._particleDataHdlrObj)
            driver.processFileStream()
            
        finally:
            stream_handle.close()
            
        return self._particleDataHdlrObj