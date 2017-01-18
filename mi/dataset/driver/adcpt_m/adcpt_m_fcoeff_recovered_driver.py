#!/usr/bin/env python

"""
@package mi.dataset.driver.adcpt_m
@file mi-dataset/mi/dataset/driver/adcpt_m/adcpt_m_fcoeff_recovered_driver.py
@author Ronald Ronquillo
@brief Recovered driver for the adcpt_m_fcoeff instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.adcpt_m_fcoeff import AdcptMFCoeffParser
from mi.core.versioning import version


@version("15.6.1")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param unused
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = AdcptMFCoeffRecoveredDriver(unused, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class AdcptMFCoeffRecoveredDriver(SimpleDatasetDriver):
    """
    The adcpt_m_fcoeff driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_m_fcoeff',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptMFCoeffInstrumentDataParticle'
        }

        parser = AdcptMFCoeffParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser
