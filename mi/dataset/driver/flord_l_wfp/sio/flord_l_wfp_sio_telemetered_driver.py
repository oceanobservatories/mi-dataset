#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Mark Worden'

import os

from mi.logging import config
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver

from mi.dataset.parser.flord_l_wfp_sio import FlordLWfpSioParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    log = get_logger()

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp_sio',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpSioDataParticle'
    }

    def exception_callback(exception):
        log.debug("ERROR: " + exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()

    with open(sourceFilePath, 'r') as stream_handle:
        parser = FlordLWfpSioParser(parser_config,
                                    stream_handle,
                                    exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj
