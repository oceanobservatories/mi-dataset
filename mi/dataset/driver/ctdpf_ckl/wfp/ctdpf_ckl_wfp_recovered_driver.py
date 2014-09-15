#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os
import sys

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.ctdpf_ckl.wfp.ctdpf_ckl_wfp_driver import CtdpfCklWfpDriver
from mi.dataset.parser.ctdpf_ckl_wfp_particles import CtdpfCklWfpRecoveredDataParticle, \
    CtdpfCklWfpRecoveredMetadataParticle

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

    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_wfp_particles',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'instrument_data_particle_class': CtdpfCklWfpRecoveredDataParticle,
            'metadata_particle_class': CtdpfCklWfpRecoveredMetadataParticle
        }
    }
    log.debug("My Config: %s", config)
    driver = CtdpfCklWfpDriver(sourceFilePath, particleDataHdlrObj, config)
        
    return driver.process()

