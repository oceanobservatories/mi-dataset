#!/usr/local/bin/python2.7

import sys
import os
from mi.core.versioning import  version

#try:
#    if basePythonCodePath is not None:
#        pass
#except NameError:
#    basePythonCodePath = os.curdir

#sys.path.append(basePythonCodePath)

@version("0.0.3")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    from mi.logging import config
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    
    from mi.core.log import get_logger
    log = get_logger()
    
    from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
    
    from mi.dataset.parser.parad_k_stc_imodem import \
        Parad_k_stc_imodemParser
    from mi.dataset.dataset_parser import DataSetDriverConfigKeys
    
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_k_stc_imodem',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'Parad_k_stc_imodemDataParticle'
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
    
    
    def exception_callback(exception):
        particleDataHdlrObj.setParticleDataCaptureFailure()
    
    
    stream_handle = open(sourceFilePath, 'rb')
    
    try:
        parser = Parad_k_stc_imodemParser(config, None, stream_handle,
                                          state_callback, pub_callback,
                                          exception_callback)
    
        driver = DataSetDriver(parser, particleDataHdlrObj)
    
        driver.processFileStream()
    
    finally:
        stream_handle.close()
    
    stream_handle = open(sourceFilePath, 'rb')
    return particleDataHdlrObj
