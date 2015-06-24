# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "mworden"

import os

from mi.core.log import get_logger
from mi.logging import config

from mi.dataset.parser.flort_dj_dcl import FlortDjDclRecoveredParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


class FlortDjDclRecoveredDriver:
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

            parser = FlortDjDclRecoveredParser(self._parser_config,
                                               file_handle,
                                               None,
                                               lambda state, ingested: None,
                                               lambda data: None,
                                               exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()

        return self._particleDataHdlrObj


@version("0.0.1")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.flort_dj_dcl",
        DataSetDriverConfigKeys.PARTICLE_CLASS: None
    }

    driver = FlortDjDclRecoveredDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
