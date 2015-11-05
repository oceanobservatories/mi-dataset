#!/usr/bin/env python

"""
@package mi.dataset.driver.adcpa_n
@file mi/dataset/driver/adcpa_n/auv/adcpa_n_recovered_driver.py
@author Jeff Roy
@brief Driver for the adcpa_n instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("15.7.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = AdcpaNRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class AdcpaNRecoveredDriver(SimpleDatasetDriver):
    """
    Derived adcpa_n driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'velocity': 'VelocityInst',
                'engineering': 'AuvEngineering',
                'config': 'AuvConfig',
                'bottom_track': 'InstBottom',
                'bottom_track_config': 'BottomConfig',
            }
        }

        parser = AdcpPd0Parser(config, stream_handle, self._exception_callback)

        return parser


