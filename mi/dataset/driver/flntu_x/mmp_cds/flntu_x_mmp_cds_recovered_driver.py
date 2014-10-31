#!/usr/bin/env python

"""
@package mi.dataset.driver.flntu_x.mmp_cds.flntu_x_mmp_cds_recovered_driver
@file mi-dataset/mi/dataset/driver/flntu_x/mmp_cds/flntu_x_mmp_cds_recovered_driver.py
@author Jeff Roy
@brief Driver for the flntu_x_mmp_cds instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flntu_x_mmp_cds import FlntuXMmpCdsParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    with open(sourceFilePath, 'rb') as stream_handle:

        driver = FlntuXMmpCdsRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class FlntuXMmpCdsRecoveredDriver(SimpleDatasetDriver):
    """
    Derived flntu_x_mmp_cds driver class
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flntu_x_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlntuXMmpCdsParserDataParticle'
        }

        parser = FlntuXMmpCdsParser(parser_config,
                                    None,
                                    stream_handle,
                                    lambda state, ingested: None,
                                    lambda data: None,
                                    self._exception_callback)

        return parser


