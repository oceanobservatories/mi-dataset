#!/usr/bin/env python

"""
@package mi.dataset.driver.WFP_ENG.wfp_sio
@file mi-dataset/mi/dataset/driver/WFP_ENG/wfp_sio/wfp_eng_wdp_sio_telemetered_driver.py
@author Mark Worden
@brief Driver for the wfp_eng_wfp_sio instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.wfp_eng_wfp_sio import WfpEngWfpSioParser
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

        driver = WfpEngWfpSioTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class WfpEngWfpSioTelemeteredDriver(SimpleDatasetDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        }

        parser = WfpEngWfpSioParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser


