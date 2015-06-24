#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Mark Worden'

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver

from mi.dataset.parser.flord_l_wfp_sio import FlordLWfpSioParser
from mi.core.versioning import version


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    with open(sourceFilePath, 'rb') as stream_handle:

        driver = FlordLWfpSioTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class FlordLWfpSioTelemeteredDriver(SimpleDatasetDriver):


    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpSioDataParticle'
        }

        parser = FlordLWfpSioParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser
