#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'jroy'

import os

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpRecoveredDataParticle, \
    DofstKWfpRecoveredMetadataParticle
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
        driver = DofstKWfpRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class DofstKWfpRecoveredDriver(SimpleDatasetDriver):
    """
    Derived dofst_k_wfp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        filesize = os.path.getsize(stream_handle.name)

        config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofs_k_wfp_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'instrument_data_particle_class': DofstKWfpRecoveredDataParticle,
                'metadata_particle_class': DofstKWfpRecoveredMetadataParticle
            }
        }
        parser = DofstKWfpParser(config,
                                 None,
                                 stream_handle,
                                 lambda state, ingested: None,
                                 lambda data: None,
                                 self._exception_callback,
                                 filesize)

        return parser
