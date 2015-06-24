"""
@package mi.dataset.driver.parad_j.cssp
@file mi-dataset/mi/dataset/driver/parad_j/cspp/parad_j_cspp_recovered_driver.py
@author Joe Padula
@brief Recovered driver for the parad_j_cspp instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cspp_base import \
    DATA_PARTICLE_CLASS_KEY, \
    METADATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.parad_j_cspp import \
    ParadJCsppParser, \
    ParadJCsppInstrumentRecoveredDataParticle, \
    ParadJCsppMetadataRecoveredDataParticle
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

    with open(sourceFilePath, 'rU') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = ParadJCsppRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)

        driver.processFileStream()

    return particleDataHdlrObj


class ParadJCsppRecoveredDriver(SimpleDatasetDriver):
    """
    The parad_j_cspp recovered driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_j_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: ParadJCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: ParadJCsppInstrumentRecoveredDataParticle,
            }
        }

        parser = ParadJCsppParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)

        return parser
