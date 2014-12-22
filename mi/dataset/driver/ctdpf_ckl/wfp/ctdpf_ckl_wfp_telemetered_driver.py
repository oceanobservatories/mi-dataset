#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os
import sys

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdpf_ckl_wfp import CtdpfCklWfpParser, \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.ctdpf_ckl_wfp_particles import \
    CtdpfCklWfpTelemeteredDataParticle, \
    CtdpfCklWfpTelemeteredMetadataParticle


class CtdpfCklWfpTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived wc_wm_cspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: CtdpfCklWfpTelemeteredMetadataParticle,
                DATA_PARTICLE_CLASS_KEY: CtdpfCklWfpTelemeteredDataParticle
            }
        }

        file_size = os.path.getsize(stream_handle.name)

        parser = CtdpfCklWfpParser(parser_config,
                                   stream_handle,
                                   self._exception_callback,
                                   file_size)

        return parser


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
        driver = CtdpfCklWfpTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj
