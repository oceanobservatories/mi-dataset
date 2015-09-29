#!/usr/bin/env python

"""
@package mi.dataset.driver.velpt_ab
@file mi-dataset/mi/dataset/driver/velpt_ab/velpt_ab_recovered_driver.py
@author Jeff Roy
@brief Recovered driver for the velpt_ab instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.velpt_ab import VelptAbParser, VelptAbParticleClassKey
from mi.dataset.parser.velpt_ab_particles import VelptAbInstrumentDataParticle, \
    VelptAbDiagnosticsHeaderParticle, VelptAbDiagnosticsDataParticle, VelptAbInstrumentMetadataParticle
from mi.core.versioning import version


@version("15.7.0")
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
        driver = VelptAbRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class VelptAbRecoveredDriver(SimpleDatasetDriver):
    """
    The velpt_ab_dcl driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDiagnosticsHeaderParticle,
                VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDiagnosticsDataParticle,
                VelptAbParticleClassKey.INSTRUMENT_METADATA_PARTICLE_CLASS: VelptAbInstrumentMetadataParticle,
                VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbInstrumentDataParticle
            }
        }

        parser = VelptAbParser(parser_config,
                               stream_handle,
                               self._exception_callback)

        return parser
