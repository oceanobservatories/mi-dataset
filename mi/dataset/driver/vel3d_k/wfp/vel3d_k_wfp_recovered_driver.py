#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'kustert'

import os
import sys

from mi.logging import config
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
from mi.dataset.parser.vel3d_k_wfp import Vel3dKWfpParser
from mi.core.versioning import version

@version("0.1.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    log = get_logger()

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_k_wfp',
        DataSetDriverConfigKeys.PARTICLE_CLASS: ['Vel3dKWfpMetadataParticle',
                                                 'Vel3dKWfpInstrumentParticle',
                                                 'Vel3dKWfpStringParticle']
    }

    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()

    with open(sourceFilePath, 'rb') as stream_handle:
        parser = Vel3dKWfpParser(parser_config,
                                 stream_handle,
                                 exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj
