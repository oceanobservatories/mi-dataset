##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Nick Almonte"

import os

from mi.logging import config

from mi.dataset.driver.phsen_abcdef.dcl.driver_common import Phsen_abcdef_dcl_Driver

from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclMetadataRecoveredDataParticle
from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclInstrumentRecoveredDataParticle
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version('0.0.1')
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.driver.phsen_abcdef.dcl',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'metadata_particle_class_key': PhsenAbcdefDclMetadataRecoveredDataParticle,
            'data_particle_class_key': PhsenAbcdefDclInstrumentRecoveredDataParticle,
        }
    }

    driver = Phsen_abcdef_dcl_Driver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()