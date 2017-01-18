#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdpf_ckl.wfp_sio.ctdpf_ckl_wfp_sio_telemetered_driver.py
@file mi-dataset/mi/dataset/driver/ctdpf_ckl/wfp_sio/ctdpf_wfp_sio_telemetered_driver.py
@author Jeff Roy
@brief Driver for the ctdpf_ckl_wfp_sio instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdpf_ckl_wfp_sio import CtdpfCklWfpSioParser
from mi.core.versioning import version


@version("0.0.2")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param unused
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = CtdpfWfpSioTelemeteredDriver(unused, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdpfWfpSioTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived ctdpf_ckl_wfp_sio driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['CtdpfCklWfpSioDataParticle',
                                                     'CtdpfCklWfpSioMetadataParticle']
        }

        parser = CtdpfCklWfpSioParser(parser_config, stream_handle,
                                      self._exception_callback)

        return parser


