# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.core.log import get_logger
from mi.logging import config

from mi.dataset.parser.spkir_abj_dcl import SpkirAbjDclTelemeteredParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

__author__ = "mworden"


class SpkirAbjDclTelemeteredDriver:
    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        with open(self._sourceFilePath, "r") as file_handle:
            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = SpkirAbjDclTelemeteredParser(self._parser_config,
                                                  file_handle,
                                                  exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()

        return self._particleDataHdlrObj


@version("15.6.2")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.spkir_abj_dcl",
        DataSetDriverConfigKeys.PARTICLE_CLASS: None
    }

    driver = SpkirAbjDclTelemeteredDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
