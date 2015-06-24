#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_stc_eng.stc.cg_stc_eng_stc_common_driver import CgStcEngDriver
from mi.core.versioning import version

@version("0.0.2")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_stc_eng_stc',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CgStcEngStcParserRecoveredDataParticle'
    }
    
    driver = CgStcEngDriver(sourceFilePath, particleDataHdlrObj, parser_config)
    
    return driver.process()