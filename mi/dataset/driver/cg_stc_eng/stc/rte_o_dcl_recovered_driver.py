##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_stc_eng.stc.driver_common import RteODclDriver

__author__ = "jpadula"


@version("0.0.4")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.rte_o_dcl',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'RteODclParserRecoveredDataParticle'
    }

    driver = RteODclDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
