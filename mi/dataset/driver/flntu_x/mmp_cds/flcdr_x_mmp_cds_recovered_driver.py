#!/usr/bin/env python

"""
@package mi.dataset.driver.flntu_x.mmp_cds.flcdr_x_mmp_cds_recovered_driver
@file mi-dataset/mi/dataset/driver/flntu_x/mmp_cds/flcdr_x_mmp_cds_recovered_driver.py
@author Jeff Roy
@brief Driver for the flcdr_x_mmp_cds instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flcdr_x_mmp_cds import FlcdrXMmpCdsParser
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
        driver = FlcdrXMmpCdsRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class FlcdrXMmpCdsRecoveredDriver(SimpleDatasetDriver):
    """
    Derived flcdr_x_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flcdr_x_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlcdrXMmpCdsParserDataParticle'
        }

        parser = FlcdrXMmpCdsParser(parser_config,
                                    None,
                                    stream_handle,
                                    lambda state, ingested: None,
                                    lambda data: None,
                                    self._exception_callback)

        return parser


