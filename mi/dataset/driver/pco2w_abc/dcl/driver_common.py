##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "jpadula"

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.pco2w_abc_dcl import Pco2wAbcDclParser


class Pco2wAbcDclDriver:
    """
    Driver class for the pco2w_abc_dcl data set.
    """

    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):
        
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):
        
        log = get_logger()

        with open(self._sourceFilePath, "rb") as file_handle:

            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = Pco2wAbcDclParser(self._parser_config,
                                       file_handle,
                                       exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()
        
        return self._particleDataHdlrObj
