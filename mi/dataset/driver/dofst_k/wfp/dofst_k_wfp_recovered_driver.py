#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'kustert'

import os
import sys

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.dofst_k.wfp.dofst_k_wfp_driver import DofstKWfpDriver
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpRecoveredDataParticle, \
    DofstKWfpRecoveredMetadataParticle

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    
    try:
        if basePythonCodePath is not None:
            pass
    except NameError:
        basePythonCodePath = os.curdir
    sys.path.append(basePythonCodePath)
    
    from mi.logging import config
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofs_k_wfp_particles',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'instrument_data_particle_class': DofstKWfpRecoveredDataParticle,
            'metadata_particle_class': DofstKWfpRecoveredMetadataParticle
        }
    }
    
    driver = DofstKWfpDriver(basePythonCodePath, sourceFilePath, particleDataHdlrObj, config)
    return driver.process()