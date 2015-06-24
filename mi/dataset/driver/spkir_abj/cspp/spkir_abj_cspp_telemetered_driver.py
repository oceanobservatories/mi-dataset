
"""
@package mi.dataset.driver.spkir_abj.cspp
@file mi/dataset/driver/spikr_abj/cspp/spkir_abj_cspp_telemetered_driver.py
@author Mark Worden
@brief Driver for the spkir_abj_cspp instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.spkir_abj_cspp import \
    SpkirAbjCsppParser, \
    SpkirAbjCsppInstrumentTelemeteredDataParticle, \
    SpkirAbjCsppMetadataTelemeteredDataParticle
from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
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

    with open(sourceFilePath, 'r') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = SpkirAbjCsppRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class SpkirAbjCsppRecoveredDriver(SimpleDatasetDriver):
    """
    Derived spkir_abj_cspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.spkir_abj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: SpkirAbjCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: SpkirAbjCsppInstrumentTelemeteredDataParticle,
            }
        }

        parser = SpkirAbjCsppParser(parser_config, stream_handle,
                                    self._exception_callback)

        return parser


