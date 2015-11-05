##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Jeff Roy"

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser


class AdcpaDriver:
    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):

        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):

        with open(self._sourceFilePath, "rb") as file_handle:

            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = AdcpPd0Parser(self._parser_config, file_handle, exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()

        return self._particleDataHdlrObj

