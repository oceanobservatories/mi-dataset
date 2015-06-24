#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_cpm_eng.cpm.cg_cpm_eng_cpm_common_driver import CgCpmEngCpmDriver
from mi.core.versioning import version

@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_cpm_eng_cpm',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CgCpmEngCpmRecoveredDataParticle'
    }

    driver = CgCpmEngCpmDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()