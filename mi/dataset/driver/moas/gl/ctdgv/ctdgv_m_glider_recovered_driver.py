##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "jpadula"

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.driver.moas.gl.ctdgv.driver_common import CtdgvMGliderDriver

from mi.core.versioning import version

@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdgvRecoveredDataParticle'
    }
    
    driver = CtdgvMGliderDriver(sourceFilePath, particleDataHdlrObj, parser_config)
        
    return driver.process()
