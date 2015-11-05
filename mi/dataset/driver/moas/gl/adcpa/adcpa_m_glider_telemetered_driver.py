##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Jeff Roy"

import os

from mi.logging import config

from mi.dataset.driver.moas.gl.adcpa.adcpa_driver_common import AdcpaDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("0.1.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'velocity': 'VelocityGlider',
            'engineering': 'GliderEngineering',
            'config': 'GliderConfig',
            'bottom_track': 'EarthBottom',
            'bottom_track_config': 'BottomConfig',
        }
    }

    driver = AdcpaDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
