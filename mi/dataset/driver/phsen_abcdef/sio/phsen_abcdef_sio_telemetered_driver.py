#!/usr/bin/env python

"""
@package mi.dataset.driver.phsen_abcdef.sio.phsen_abcdef_sio_telemetered_driver
@file mi-dataset/mi/dataset/driver/phsen_abcdef/sio/phsen_abcdef_sio_telemetered_driver.py
@author Emily Hahn
@brief Driver for the phsen_abcdef instrument
"""

__author__ = 'ehahn'
__license__ = 'Apache 2.0'

import os

from mi.logging import config
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.phsen_abcdef_sio import PhsenAbcdefSioParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_sio',
        DataSetDriverConfigKeys.PARTICLE_CLASS: ['PhsenAbcdefSioDataParticle',
                                                 'PhsenAbcdefSioControlDataParticle']
    }

    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()

    with open(sourceFilePath, 'rb') as stream_handle:
        parser = PhsenAbcdefSioParser(parser_config, stream_handle, exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj

