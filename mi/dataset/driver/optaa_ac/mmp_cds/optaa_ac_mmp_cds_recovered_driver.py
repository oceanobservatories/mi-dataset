#!/usr/bin/env python

"""
@package mi.dataset.driver.optaa_ac.mmp_cds.optaa_ac_mmp_cds_recovered_driver
@file mi-dataset/mi/dataset/driver/optaa_ac/mmp_cds/optaa_ac_mmp_cds_recovered_driver.py
@author Jeff Roy
@brief Driver for the optaa_ac_mmp_cds instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.core.versioning import version


@version("0.0.3")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param unused
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """
    with open(sourceFilePath, 'rb') as stream_handle:

        driver = OptaaAcMmpCdsRecoveredDriver(unused, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class OptaaAcMmpCdsRecoveredDriver(SimpleDatasetDriver):
    """
    Derived optaa_ac_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """
    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.optaa_ac_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'OptaaAcMmpCdsParserDataParticle'
        }

        parser = MmpCdsParser(parser_config, stream_handle, self._exception_callback)

        return parser


