#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
import os

from mi.logging import config
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_stc_eng.stc.mopak_o_dcl_common_driver import MopakDriver
from mi.dataset.parser.mopak_o_dcl import \
    MopakODclAccelParserRecoveredDataParticle, \
    MopakODclRateParserRecoveredDataParticle, \
    MopakParticleClassType
from mi.core.versioning import version

@version("0.0.2")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.mopak_o_dcl',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        # particle_class configuration does nothing for multi-particle parsers
        # put the class names in specific config parameters so the parser can get them
        # use real classes as objects instead of strings to make it easier
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT:
            {MopakParticleClassType.ACCEL_PARTCICLE_CLASS: MopakODclAccelParserRecoveredDataParticle,
             MopakParticleClassType.RATE_PARTICLE_CLASS: MopakODclRateParserRecoveredDataParticle}
    }
    
    driver = MopakDriver(sourceFilePath, particleDataHdlrObj, parser_config)
    
    return driver.process()