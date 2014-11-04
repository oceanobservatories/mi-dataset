#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Ronald Ronquillo"

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.adcpt_acfgm_dcl_pd8 import AdcptAcfgmPd8Parser


MODULE_NAME = 'mi.dataset.parser.adcpt_acfgm_dcl_pd8'
ADCPT_ACFGM_DCL_PD8_RECOVERED_PARTICLE_CLASS = 'AdcptAcfgmPd8DclInstrumentRecoveredParticle'
ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS = 'AdcptAcfgmPd8DclInstrumentParticle'


class AdcptAcfgmDclPd8Driver:

    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):
        
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):
        
        with open(self._sourceFilePath, "r") as file_handle:

            def exception_callback(exception):
                log.trace("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = AdcptAcfgmPd8Parser(self._parser_config,
                                         file_handle,
                                         exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()

        return self._particleDataHdlrObj

