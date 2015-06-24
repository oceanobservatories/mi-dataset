##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "mworden"

import os

from mi.logging import config

from mi.dataset.driver.WFP_ENG.STC_IMODEM.driver_common import WfpEngStcImodemDriver

from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStatusTelemeteredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStartTelemeteredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemEngineeringTelemeteredDataParticle
    
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("0.0.1")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.driver.WFP_ENG.STC_IMODEM',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'status_data_particle_class': WfpEngStcImodemStatusTelemeteredDataParticle,
            'start_data_particle_class': WfpEngStcImodemStartTelemeteredDataParticle,
            'engineering_data_particle_class': WfpEngStcImodemEngineeringTelemeteredDataParticle
        }
    }
    
    driver = WfpEngStcImodemDriver(sourceFilePath, particleDataHdlrObj, parser_config)
        
    return driver.process()

