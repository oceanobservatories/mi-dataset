"""
@package mi.dataset.driver.nutnr_j.cspp
@file mi-dataset/mi/dataset/driver/nutnr_j/cspp/nutnr_j_cspp_telemetered_driver.py
@author Joe Padula
@brief Telemetered driver for the nutnr_j_cspp instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.cspp_base import \
    DATA_PARTICLE_CLASS_KEY, \
    METADATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.nutnr_j_cspp import \
    NutnrJCsppMetadataTelemeteredDataParticle, \
    NutnrJCsppTelemeteredDataParticle, \
    NutnrJCsppParser
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

    with open(sourceFilePath, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = NutnrJCsppTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)

        driver.processFileStream()

    return particleDataHdlrObj


class NutnrJCsppTelemeteredDriver(SimpleDatasetDriver):
    """
    The nutnr_j_cspp telemetered driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: NutnrJCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: NutnrJCsppTelemeteredDataParticle
            }
        }

        parser = NutnrJCsppParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)
        return parser
