#!/usr/bin/env python

"""
@package mi.dataset.driver.nutnr_m.glider
@file mi/dataset/driver/nutnr_m/glider/nutnr_m_glider_telemetered_driver.py
@author Emily Hahn
@brief Driver for the nutnr series m instrument on a glider
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.glider import GliderParser
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

    with open(sourceFilePath, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = NutnrMGliderTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class NutnrMGliderTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived nutnr_m_glider driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'NutnrMDataParticle'
        }
        return GliderParser(config, stream_handle, self._exception_callback)


