
"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/dosta_abcdjm_cspp.py
@author Mark Worden
@brief Parser for the dosta_abcdjm_cspp dataset driver
Release notes:

initial release
"""

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

import numpy

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle
from mi.dataset.parser.common_regexes import INT_REGEX, FLOAT_REGEX, MULTIPLE_TAB_REGEX, END_OF_LINE_REGEX
from mi.dataset.parser.cspp_base import CsppParser, Y_OR_N_REGEX, CsppMetadataDataParticle, MetadataRawDataKey, \
    encode_y_or_n


# A regular expression for special characters that could exist in a data record preceding the model
SPECIAL_CHARS_REGEX = r'(?:[\?][%])?'

# A regular expression that should match a dosta_abcdjm data record
DATA_REGEX = r'(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX   # Profiler Timestamp
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX   # Depth
DATA_REGEX += '(' + Y_OR_N_REGEX + ')' + MULTIPLE_TAB_REGEX  # Suspect Timestamp
DATA_REGEX += SPECIAL_CHARS_REGEX + '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX  # Model Number
DATA_REGEX += '(' + INT_REGEX + ')' + MULTIPLE_TAB_REGEX    # Serial Number
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # oxygen content
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # ambient temperature
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # calibrated phase
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # temperature compensated phase
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # phase measurement with blue excitation
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # phase measurement with red excitation
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # amplitude measurement with blue excitation
DATA_REGEX += '(' + FLOAT_REGEX + ')' + MULTIPLE_TAB_REGEX  # amplitude measurement with red excitation
DATA_REGEX += '(' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX   # raw temperature, voltage from thermistor


class DataMatchesGroupNumber(BaseEnum):
    """
    An enum for group match indices for a data record only chunk.
    """
    PROFILER_TIMESTAMP = 1
    PRESSURE = 2
    SUSPECT_TIMESTAMP = 3
    MODEL = 4
    SERIAL_NUMBER = 5
    ESTIMATED_OXYGEN_CONCENTRATION = 6
    OPTODE_TEMPERATURE = 7
    CALIBRATED_PHASE = 8
    TEMP_COMPENSATED_PHASE = 9
    BLUE_PHASE = 10
    RED_PHASE = 11
    BLUE_AMPLITUDE = 12
    RED_AMPLITUDE = 13
    RAW_TEMPERATURE = 14


class DataParticleType(BaseEnum):
    """
    The data particle types that a dosta_abcdjm_cspp parser could generate
    """
    METADATA_RECOVERED = 'dosta_abcdjm_cspp_metadata_recovered'
    INSTRUMENT_RECOVERED = 'dosta_abcdjm_cspp_instrument_recovered'
    METADATA_TELEMETERED = 'dosta_abcdjm_cspp_metadata'
    INSTRUMENT_TELEMETERED = 'dosta_abcdjm_cspp_instrument'


class DostaAbcdjmCsppParserDataParticleKey(BaseEnum):
    """
    The data particle keys associated with dosta_abcdjm_cspp data particle parameters
    """
    PRODUCT_NUMBER = 'product_number'
    SERIAL_NUMBER = 'serial_number'
    PROFILER_TIMESTAMP = 'profiler_timestamp'
    PRESSURE = 'pressure_depth'
    SUSPECT_TIMESTAMP = 'suspect_timestamp'
    ESTIMATED_OXYGEN_CONCENTRATION = 'estimated_oxygen_concentration'
    OPTODE_TEMPERATURE = 'optode_temperature'
    CALIBRATED_PHASE = 'calibrated_phase'
    TEMP_COMPENSATED_PHASE = 'temp_compensated_phase'
    BLUE_PHASE = 'blue_phase'
    RED_PHASE = 'red_phase'
    BLUE_AMPLITUDE = 'blue_amplitude'
    RED_AMPLITUDE = 'red_amplitude'
    RAW_TEMPERATURE = 'raw_temperature'


# A group of non common metadata particle encoding rules used to simplify encoding using a loop
NON_COMMON_METADATA_PARTICLE_ENCODING_RULES = [
    (DostaAbcdjmCsppParserDataParticleKey.PRODUCT_NUMBER, DataMatchesGroupNumber.MODEL, int),
    (DostaAbcdjmCsppParserDataParticleKey.SERIAL_NUMBER, DataMatchesGroupNumber.SERIAL_NUMBER, str)
]

# A group of instrument data particle encoding rules used to simplify encoding using a loop
INSTRUMENT_PARTICLE_ENCODING_RULES = [
    (DostaAbcdjmCsppParserDataParticleKey.PROFILER_TIMESTAMP, DataMatchesGroupNumber.PROFILER_TIMESTAMP, numpy.float),
    (DostaAbcdjmCsppParserDataParticleKey.PRESSURE, DataMatchesGroupNumber.PRESSURE, float),
    (DostaAbcdjmCsppParserDataParticleKey.SUSPECT_TIMESTAMP, DataMatchesGroupNumber.SUSPECT_TIMESTAMP, encode_y_or_n),
    (DostaAbcdjmCsppParserDataParticleKey.ESTIMATED_OXYGEN_CONCENTRATION,
     DataMatchesGroupNumber.ESTIMATED_OXYGEN_CONCENTRATION, float),
    (DostaAbcdjmCsppParserDataParticleKey.OPTODE_TEMPERATURE, DataMatchesGroupNumber.OPTODE_TEMPERATURE, float),
    (DostaAbcdjmCsppParserDataParticleKey.CALIBRATED_PHASE, DataMatchesGroupNumber.CALIBRATED_PHASE, float),
    (DostaAbcdjmCsppParserDataParticleKey.TEMP_COMPENSATED_PHASE, DataMatchesGroupNumber.TEMP_COMPENSATED_PHASE, float),
    (DostaAbcdjmCsppParserDataParticleKey.BLUE_PHASE, DataMatchesGroupNumber.BLUE_PHASE, float),
    (DostaAbcdjmCsppParserDataParticleKey.RED_PHASE, DataMatchesGroupNumber.RED_PHASE, float),
    (DostaAbcdjmCsppParserDataParticleKey.BLUE_AMPLITUDE, DataMatchesGroupNumber.BLUE_AMPLITUDE, float),
    (DostaAbcdjmCsppParserDataParticleKey.RED_AMPLITUDE, DataMatchesGroupNumber.RED_AMPLITUDE, float),
    (DostaAbcdjmCsppParserDataParticleKey.RAW_TEMPERATURE, DataMatchesGroupNumber.RAW_TEMPERATURE, float),
]


class DostaAbcdjmCsppMetadataDataParticle(CsppMetadataDataParticle):
    """
    Class for building a dosta_abcdjm_cspp metadata particle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        # Set the base metadata parsed values to the results to return
        results = self._build_metadata_parsed_values()

        data_match = self.raw_data[MetadataRawDataKey.DATA_MATCH]

        # Process each of the non common metadata particle parameters
        for (name, index, encoding) in NON_COMMON_METADATA_PARTICLE_ENCODING_RULES:
            results.append(self._encode_value(name, data_match.group(index), encoding))

        # Set the internal timestamp
        internal_timestamp_unix = numpy.float(data_match.group(
            DataMatchesGroupNumber.PROFILER_TIMESTAMP))
        self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        return results


class DostaAbcdjmCsppMetadataRecoveredDataParticle(DostaAbcdjmCsppMetadataDataParticle):
    """
    Class for building a dosta_abcdjm_cspp recovered metadata particle
    """

    _data_particle_type = DataParticleType.METADATA_RECOVERED


class DostaAbcdjmCsppMetadataTelemeteredDataParticle(DostaAbcdjmCsppMetadataDataParticle):
    """
    Class for building a dosta_abcdjm_cspp telemetered metadata particle
    """

    _data_particle_type = DataParticleType.METADATA_TELEMETERED


class DostaAbcdjmCsppInstrumentDataParticle(DataParticle):
    """
    Class for building a dosta_abcdjm_cspp instrument data particle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        results = []

        # Process each of the instrument particle parameters
        for (name, index, encoding) in INSTRUMENT_PARTICLE_ENCODING_RULES:
            results.append(self._encode_value(name, self.raw_data.group(index), encoding))

        # # Set the internal timestamp
        internal_timestamp_unix = numpy.float(self.raw_data.group(
            DataMatchesGroupNumber.PROFILER_TIMESTAMP))
        self.set_internal_timestamp(unix_time=internal_timestamp_unix)

        return results


class DostaAbcdjmCsppInstrumentRecoveredDataParticle(DostaAbcdjmCsppInstrumentDataParticle):
    """
    Class for building a dosta_abcdjm_cspp recovered instrument data particle
    """

    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED


class DostaAbcdjmCsppInstrumentTelemeteredDataParticle(DostaAbcdjmCsppInstrumentDataParticle):
    """
    Class for building a dosta_abcdjm_cspp telemetered instrument data particle
    """

    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED


class DostaAbcdjmCsppParser(CsppParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        """
        This method is a constructor that will instantiate an DostaAbcdjmCsppParser object.
        @param config The configuration for this DostaAbcdjmCsppParser parser
        @param stream_handle The handle to the data stream containing the dosta_abcdjm_cspp data
        @param exception_callback The function to call to report exceptions
        """

        # Call the superclass constructor
        super(DostaAbcdjmCsppParser, self).__init__(config,
                                                    stream_handle,
                                                    exception_callback,
                                                    DATA_REGEX)
