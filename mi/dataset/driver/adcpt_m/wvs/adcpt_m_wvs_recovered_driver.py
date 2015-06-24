#!/usr/bin/env python

"""
@package mi.dataset.driver.adcpt_m.wvs
@file mi-dataset/mi/dataset/driver/adcpt_m/wvs/adcpt_m_wvs_recovered_driver.py
@author Ronald Ronquillo
@brief Recovered driver for the adcpt_m_wvs instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.adcpt_m_wvs import AdcptMWVSParser
from mi.core.versioning import version


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = AdcptMWVSRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class AdcptMWVSRecoveredDriver(SimpleDatasetDriver):
    """
    The adcpt_m_wvs driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_m_wvs',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptMWVSInstrumentDataParticle'
        }

        parser = AdcptMWVSParser(parser_config,
                                 stream_handle,
                                 self._exception_callback)

        return parser
