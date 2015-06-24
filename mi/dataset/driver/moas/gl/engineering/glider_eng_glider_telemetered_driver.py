# #
# OOIPLACEHOLDER
#
# #

__author__ = "ehahn"

import os

from mi.logging import config

from mi.dataset.driver.moas.gl.engineering.driver_common import GliderEngineeringDriver
from mi.dataset.parser.glider import EngineeringClassKey

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.core.versioning import version

@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    Initialize the parser configuration and build the driver
    @param basePythonCodePath - python code path from Java
    @param sourceFilePath - source file from Java
    @param particleDataHdlrObj - particle data handler object from Java
    """
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            EngineeringClassKey.METADATA: 'EngineeringMetadataDataParticle',
            EngineeringClassKey.DATA: 'EngineeringTelemeteredDataParticle',
            EngineeringClassKey.SCIENCE: 'EngineeringScienceTelemeteredDataParticle'
        }
    }

    driver = GliderEngineeringDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()