##
# OOIPLACEHOLDER
#
##


import os

from mi.logging import config

from mi.dataset.driver.moas.gl.engineering.driver_common import GliderEngineeringDriver
from mi.dataset.parser.glider import EngineeringClassKey

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.core.versioning import version

__author__ = "ehahn"


@version("15.7.1")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    """
    Initialize the parser configuration and build the driver
    @param unused - python code path from Java
    @param sourceFilePath - source file from Java
    @param particleDataHdlrObj - particle data handler object from Java
    """

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            EngineeringClassKey.METADATA: 'EngineeringMetadataRecoveredDataParticle',
            EngineeringClassKey.DATA: 'EngineeringRecoveredDataParticle',
            EngineeringClassKey.SCIENCE: 'EngineeringScienceRecoveredDataParticle',
            EngineeringClassKey.GPS: 'GpsPositionDataParticle'
        }
    }

    driver = GliderEngineeringDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
