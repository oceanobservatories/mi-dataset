#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Ronald Ronquillo"

import os

from mi.logging import config

from mi.dataset.driver.adcpt_acfgm.dcl.pd8.adcpt_acfgm_dcl_pd8_driver_common import \
    AdcptAcfgmDclPd8Driver, MODULE_NAME, ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE:  MODULE_NAME,
        DataSetDriverConfigKeys.PARTICLE_CLASS: ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS,
    }

    driver = AdcptAcfgmDclPd8Driver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
