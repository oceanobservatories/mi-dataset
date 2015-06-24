"""
@package mi.dataset.driver.optaa_dj.cspp
@file mi-dataset/mi/dataset/driver/optaa_dj/cspp/optaa_dj_cspp_telemetered_driver.py
@author Joe Padula
@brief Telemetered driver for the optaa_dj_cspp instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cspp_base import \
    DATA_PARTICLE_CLASS_KEY, \
    METADATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.optaa_dj_cspp import \
    OptaaDjCsppParser, \
    OptaaDjCsppMetadataTelemeteredDataParticle, \
    OptaaDjCsppInstrumentTelemeteredDataParticle
from mi.core.versioning import version


@version("15.6.0")
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
        driver = OptaaDjCsppTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)

        driver.processFileStream()

    return particleDataHdlrObj


class OptaaDjCsppTelemeteredDriver(SimpleDatasetDriver):
    """
    The optaa_dj_cspp telemetered driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.optaa_dj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: OptaaDjCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: OptaaDjCsppInstrumentTelemeteredDataParticle
            }
        }

        parser = OptaaDjCsppParser(parser_config,
                                   stream_handle,
                                   self._exception_callback)
        return parser
