"""
@package mi.dataset.driver.wavss_a.dcl.wavss_a_dcl_telemetered
@file mi/dataset/driver/wavss_a/dcl/wavss_a_dcl_telemetered.py
@author Emily Hahn
@brief A driver for the recovered wavss series a instrument through a DCL
"""
__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import os

from mi.logging import config

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.wavss_a_dcl import WavssADclParser


class WavssADclRecoveredDriver:

    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):
        """
        Initialize the wavss a dcl telemetered driver
        @param sourceFilePath - source file from Java
        @param particleDataHdlrObj - particle data handler object from Java
        @param parser_config - parser configuration dictionary
        """
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):
        """
        Process a file by opening the file and instantiating a parser and driver
        @return: processed particle data handler object
        """
        log = get_logger()

        with open(self._sourceFilePath, "r") as file_handle:
            def exception_callback(exception):
                log.warn("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            # instantiate the parser
            parser = WavssADclParser(self._parser_config, file_handle, exception_callback, is_telemetered=False)
            # instantiate the driver
            driver = DataSetDriver(parser, self._particleDataHdlrObj)
            # start the driver processing the file
            driver.processFileStream()

        return self._particleDataHdlrObj


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    Initialize the parser configuration and build the driver
    @param basePythonCodePath: python code path from Java
    @param sourceFilePath: source file from Java
    @param particleDataHdlrObj: particle data handler object from Java
    @return: processed particle data handler object
    """
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    # no parser config required
    parser_config = {}
    driver = WavssADclRecoveredDriver(sourceFilePath, particleDataHdlrObj, parser_config)
    return driver.process()