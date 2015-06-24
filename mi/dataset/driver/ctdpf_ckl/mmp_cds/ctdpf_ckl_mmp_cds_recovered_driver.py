#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdpf_ckl.mmp_cds.ctdpf_ckl_mmp_cds_recovered_driver.py
@file mi-dataset/mi/dataset/driver/ctdpf_ckl/mmp_cds/fctdpf_ckl_mmp_cds_recovered_driver.py
@author Jeff Roy
@brief Driver for the ctdpf_ckl_mmp_cds instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdpf_ckl_mmp_cds import CtdpfCklMmpCdsParser
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

    with open(sourceFilePath, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = CtdpfCklMmpCdsRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdpfCklMmpCdsRecoveredDriver(SimpleDatasetDriver):
    """
    Derived ctdpf_ckl_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdpfCklMmpCdsParserDataParticle'
        }

        parser = CtdpfCklMmpCdsParser(parser_config,
                                      None,
                                      stream_handle,
                                      lambda state, ingested: None,
                                      lambda data: None)

        return parser


