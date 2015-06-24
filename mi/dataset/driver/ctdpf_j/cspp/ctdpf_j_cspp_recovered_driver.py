#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdpf_j.cspp
@file mi/dataset/driver/ctdpf_j/cspp/ctdpf_j_cspp_recovered_driver.py
@author Chris Goodrich
@brief Driver for the ctdpf_j_cspp instrument
Release notes:
Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdpf_j_cspp import \
    CtdpfJCsppParser, \
    CtdpfJCsppMetadataRecoveredDataParticle, \
    CtdpfJCsppInstrumentRecoveredDataParticle
from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.core.versioning import version


@version("0.0.1")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'r') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = CtdpfJCsppRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdpfJCsppRecoveredDriver(SimpleDatasetDriver):
    """
    Derived wc_wm_cspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: CtdpfJCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: CtdpfJCsppInstrumentRecoveredDataParticle
            }
        }

        parser = CtdpfJCsppParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)

        return parser

