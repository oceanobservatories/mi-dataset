"""
@package mi.dataset.driver.nutnr_j.cspp
@file mi-dataset/mi/dataset/driver/nutnr_j/cspp/nutnr_j_cspp_recovered_driver.py
@author Joe Padula
@brief Recovered driver for the nutnr_j_cspp instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.cspp_base import METADATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.nutnr_j_cspp import \
    NutnrJCsppMetadataRecoveredDataParticle, \
    NutnrJCsppRecoveredDataParticle, \
    NutnrJCsppDarkRecoveredDataParticle, \
    NutnrJCsppParser, \
    LIGHT_PARTICLE_CLASS_KEY, \
    DARK_PARTICLE_CLASS_KEY
from mi.core.versioning import version

__author__ = 'jpadula'


@version("15.7.2")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param unused
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = NutnrJCsppRecoveredDriver(unused, stream_handle, particleDataHdlrObj)

        driver.processFileStream()

    return particleDataHdlrObj


class NutnrJCsppRecoveredDriver(SimpleDatasetDriver):
    """
    The nutnr_j_cspp recovered driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: NutnrJCsppMetadataRecoveredDataParticle,
                LIGHT_PARTICLE_CLASS_KEY: NutnrJCsppRecoveredDataParticle,
                DARK_PARTICLE_CLASS_KEY: NutnrJCsppDarkRecoveredDataParticle
            }
        }

        parser = NutnrJCsppParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)
        return parser
