#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import sys
import os

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    try:
        if basePythonCodePath is not None:
            pass
    except NameError:
        basePythonCodePath = os.curdir
    
    sys.path.append(basePythonCodePath)
    
    from mi.logging import config
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    
    from mi.core.log import get_logger
    log = get_logger()
    
    from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
    
    from mi.dataset.parser.ctdpf_ckl_mmp_cds import CtdpfCklMmpCdsParser
    from mi.dataset.dataset_parser import DataSetDriverConfigKeys
    
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_mmp_cds',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdpfCklMmpCdsParserDataParticle'
    }
    
    try:
        if particleDataHdlrObj is not None:
            pass
    except NameError:
        particleDataHdlrObj = ParticleDataHandler()
    
    try:
        if sourceFilePath is not None:
            pass
    except NameError:
        try:
            sourceFilePath = sys.argv[1]
        except IndexError:
            print "Need a source file path"
            sys.exit(1)
    
    
    def state_callback(state, ingested):
        pass
    
    
    def pub_callback(data):
        log.trace("Found data: %s", data)
    
    
    stream_handle = open(sourceFilePath, 'rb')
    
    try:
        parser = CtdpfCklMmpCdsParser(config, None, stream_handle,
                                      state_callback, pub_callback)
    
        driver = DataSetDriver(parser, particleDataHdlrObj)
    
        driver.processFileStream()
    
    finally:
        stream_handle.close()
        
    return particleDataHdlrObj
