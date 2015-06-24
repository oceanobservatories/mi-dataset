##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "jpadula"

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.driver.cg_stc_eng.stc.driver_common import RteODclDriver

from mi.core.versioning import version


@version("0.0.2")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.rte_o_dcl',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'RteODclParserRecoveredDataParticle'
    }

    driver = RteODclDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
