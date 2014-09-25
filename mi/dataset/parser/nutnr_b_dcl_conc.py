#!/usr/bin/env python

"""
@package mi.dataset.parser.nutnr_b_dcl_conc
@file marine-integrations/mi/dataset/parser/nutnr_b_dcl_conc.py
@author Steve Myerson (Raytheon)
@brief Parser for the nutnr_b_dcl_conc dataset driver

This file contains code for the nutnr_b_dcl_conc parsers and code to produce data particles.
For telemetered data, there is one parser which produces two types of data particles.
For recovered data, there is one parser which produces two types of data particles.
Both parsers produce instrument and metadata data particles.
There is 1 metadata data particle produced for each data block in a file.
There may be 1 or more data blocks in a file.
There is 1 instrument data particle produced for each record in a file.
The input files and the content of the data particles are the same for both
recovered and telemetered.
Only the names of the output particle streams are different.

Input files are ASCII with variable length records.
Release notes:

Initial release
"""

__author__ = 'Steve Myerson (Raytheon)'
__license__ = 'Apache 2.0'

import calendar
from functools import partial
import re

from mi.core.instrument.chunker import \
    StringChunker

from mi.core.log import get_logger; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.exceptions import \
    RecoverableSampleException, \
    UnexpectedDataException

from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.dataset.dataset_parser import BufferLoadingParser

# Basic patterns
ANY_CHARS = r'.*'            # Any characters excluding a newline
NEW_LINE = r'(?:\r\n|\n)'    # any type of new line
FLOAT = r'([+-]?\d+\.\d*)'   # optionally signed floating point number
SPACE = ' '

# DCL-Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Inst-Timestamp: MM/DD/YYYY HH:MM:SS (Instrument timestamp)
# All records end with one of the newlines.
DCL_DATE = r'(\d{4})/(\d{2})/(\d{2})'            # DCL Date: YYYY/MM/DD
DCL_TIME = r'(\d{2}):(\d{2}):(\d{2})\.(\d{3})'   # DCL Time: HH:MM:SS.mmm
INST_DATE = r'(\d{2})/(\d{2})/(\d{4})'           # Instrument Date: MM/DD/YYYY
INST_TIME = r'(\d{2}):(\d{2}):(\d{2})'           # Instrument Time: HH:MM:SS
DCL_TIMESTAMP = '(' + DCL_DATE + SPACE + DCL_TIME + ')'
INST_TIMESTAMP = INST_DATE + SPACE + INST_TIME
SEPARATOR = ','
START_METADATA = r'\['
END_METADATA = r'\]'

# All nutnr records are ASCII characters separated by a newline.
# This pattern is what the sieve function uses.
NUTNR_RECORD_REGEX = ANY_CHARS      # Any number of ASCII characters
NUTNR_RECORD_REGEX += NEW_LINE      # separated by a new line
NUTNR_RECORD_MATCHER = re.compile(NUTNR_RECORD_REGEX)

# Metadata records:
#   Record-Timestamp [<text>] <more text> (Some of these records are ignored)
#   Record-Timestamp DCL-Timestamp: Message <text> (Metadata fields)

META_MESSAGE_REGEX = DCL_TIMESTAMP + SPACE   # Record Timestamp
META_MESSAGE_REGEX += INST_TIMESTAMP + ': '  # DCL Timestamp
META_MESSAGE_REGEX += 'Message: '            # Indicates useful metadata record
META_MESSAGE_MATCHER = re.compile(META_MESSAGE_REGEX)

IDLE_TIME_REGEX = DCL_TIMESTAMP + SPACE   # Record Timestamp
IDLE_TIME_REGEX += START_METADATA         # Start of metadata
IDLE_TIME_REGEX += ANY_CHARS              # Any text
IDLE_TIME_REGEX += END_METADATA + ':'     # End of metadata
IDLE_TIME_REGEX += 'Idle state, without initialize'
IDLE_TIME_REGEX += NEW_LINE
IDLE_TIME_MATCHER = re.compile(IDLE_TIME_REGEX)

START_TIME_REGEX = META_MESSAGE_REGEX
START_TIME_REGEX += 'ISUS Awakened on Schedule at UTC '
START_TIME_REGEX += INST_DATE + SPACE + INST_TIME + '.'
START_TIME_REGEX += NEW_LINE
START_TIME_MATCHER = re.compile(START_TIME_REGEX)

SPEC_ON_TIME_REGEX = META_MESSAGE_REGEX
SPEC_ON_TIME_REGEX += 'Turning ON Spectrometer.'
SPEC_ON_TIME_REGEX += NEW_LINE
SPEC_ON_TIME_MATCHER = re.compile(SPEC_ON_TIME_REGEX)

SPEC_POWERED_TIME_REGEX = META_MESSAGE_REGEX
SPEC_POWERED_TIME_REGEX += 'Spectrometer powered up.'
SPEC_POWERED_TIME_REGEX += NEW_LINE
SPEC_POWERED_TIME_MATCHER = re.compile(SPEC_POWERED_TIME_REGEX)

LAMP_ON_TIME_REGEX = META_MESSAGE_REGEX
LAMP_ON_TIME_REGEX += 'Turning ON UV light source.'
LAMP_ON_TIME_REGEX += NEW_LINE
LAMP_ON_TIME_MATCHER = re.compile(LAMP_ON_TIME_REGEX)

LAMP_POWERED_TIME_REGEX = META_MESSAGE_REGEX
LAMP_POWERED_TIME_REGEX += 'UV light source powered up.'
LAMP_POWERED_TIME_REGEX += NEW_LINE
LAMP_POWERED_TIME_MATCHER = re.compile(LAMP_POWERED_TIME_REGEX)

# Filename can be any of the following formats:
#   just_a_filename
#   directory/filename       (unix style)
#   directory\filename       (windows style)
LOG_FILE_REGEX = META_MESSAGE_REGEX
LOG_FILE_REGEX += "Data log file is '"
LOG_FILE_REGEX += r'([\w\.\\/]+)'    # This is the Filename
LOG_FILE_REGEX += "'"
LOG_FILE_REGEX += r'\.'
LOG_FILE_REGEX += NEW_LINE
LOG_FILE_MATCHER = re.compile(LOG_FILE_REGEX)

NEXT_WAKEUP_REGEX = META_MESSAGE_REGEX
NEXT_WAKEUP_REGEX += "ISUS Next Wakeup at UTC "
NEXT_WAKEUP_REGEX += INST_TIMESTAMP
NEXT_WAKEUP_REGEX += r'\.'
NEXT_WAKEUP_REGEX += NEW_LINE
NEXT_WAKEUP_MATCHER = re.compile(NEXT_WAKEUP_REGEX)

# The META_MESSAGE_MATCHER produces the following groups (used in group(xxx)):
META_GROUP_DCL_TIMESTAMP = 1
META_GROUP_DCL_YEAR = 2
META_GROUP_DCL_MONTH = 3
META_GROUP_DCL_DAY = 4
META_GROUP_DCL_HOUR = 5
META_GROUP_DCL_MINUTE = 6
META_GROUP_DCL_SECOND = 7
META_GROUP_DCL_MILLISECOND = 8
META_GROUP_INST_MONTH = 9
META_GROUP_INST_DAY = 10
META_GROUP_INST_YEAR = 11
META_GROUP_INST_HOUR = 12
META_GROUP_INST_MINUTE = 13
META_GROUP_INST_SECOND = 14
META_GROUP_INST_LOGFILE = 15

# Instrument data records:
#   Record-Timestamp SAT<NDC or NLC>Serial-Number,<comma separated data>

INST_DATA_REGEX = DCL_TIMESTAMP + SPACE   # Record-Timestamp
INST_DATA_REGEX += '(SAT)'                # frame header
INST_DATA_REGEX += '([a-zA-Z]{3})'        # frame type
INST_DATA_REGEX += '([0-9a-zA-Z]{4})'     # serial number
INST_DATA_REGEX += SEPARATOR
INST_DATA_REGEX += '(\d{7})' + SEPARATOR  # julian date sample was recorded
INST_DATA_REGEX += FLOAT + SEPARATOR      # decimal time of day
INST_DATA_REGEX += FLOAT + SEPARATOR      # nitrate concentration
INST_DATA_REGEX += FLOAT + SEPARATOR      # first fitting result
INST_DATA_REGEX += FLOAT + SEPARATOR      # second fitting result
INST_DATA_REGEX += FLOAT + SEPARATOR      # third fitting result
INST_DATA_REGEX += FLOAT + NEW_LINE       # RMS error
INST_DATA_MATCHER = re.compile(INST_DATA_REGEX)

# The INST_DATA_MATCHER produces the following groups (used in group(xxx)):
INST_GROUP_DCL_TIMESTAMP = 1
INST_GROUP_DCL_YEAR = 2
INST_GROUP_DCL_MONTH = 3
INST_GROUP_DCL_DAY = 4
INST_GROUP_DCL_HOUR = 5
INST_GROUP_DCL_MINUTE = 6
INST_GROUP_DCL_SECOND = 7
INST_GROUP_DCL_MILLISECOND = 8
INST_GROUP_FRAME_HEADER = 9
INST_GROUP_FRAME_TYPE = 10
INST_GROUP_SERIAL_NUMBER = 11
INST_GROUP_JULIAN_DATE = 12
INST_GROUP_TIME_OF_DAY = 13
INST_GROUP_NITRATE = 14
INST_GROUP_FITTING1 = 15
INST_GROUP_FITTING2 = 16
INST_GROUP_FITTING3 = 17
INST_GROUP_RMS_ERROR = 18

DARK = 'NDC'      # frame type Dark
LIGHT = 'NLC'     # frame type Light

METADATA_STATE_TABLE = [
    [0x01, START_TIME_MATCHER, 0],
    [0x02, SPEC_ON_TIME_MATCHER, 0],
    [0x04, SPEC_POWERED_TIME_MATCHER, 0],
    [0x08, LAMP_ON_TIME_MATCHER, 0],
    [0x10, LAMP_POWERED_TIME_MATCHER, 0],
    [0x20, LOG_FILE_MATCHER, 0]
]
ALL_METADATA_RECEIVED = 0x3F
INDEX_VALUE = 2

# Indices into raw_data for Instrument particles.
RAW_INDEX_INST_DCL_TIMESTAMP = 0
RAW_INDEX_INST_FRAME_HEADER = 1
RAW_INDEX_INST_FRAME_TYPE = 2
RAW_INDEX_INST_SERIAL_NUMBER = 3
RAW_INDEX_INST_DATE_OF_SAMPLE = 4
RAW_INDEX_INST_TIME_OF_SAMPLE = 5
RAW_INDEX_INST_NITRATE = 6
RAW_INDEX_INST_FITTING1 = 7
RAW_INDEX_INST_FITTING2 = 8
RAW_INDEX_INST_FITTING3 = 9
RAW_INDEX_INST_RMS_ERROR = 10
RAW_INDEX_INST_YEAR = 11
RAW_INDEX_INST_MONTH = 12
RAW_INDEX_INST_DAY = 13
RAW_INDEX_INST_HOUR = 14
RAW_INDEX_INST_MINUTE = 15
RAW_INDEX_INST_SECOND = 16

# This table is used in the generation of the Instrument data particle.
# Column 1 - particle parameter name
# Column 2 - index into raw_data
# Column 3 - data encoding function (conversion required - int, float, etc)
INSTRUMENT_PARTICLE_MAP = [
    ('dcl_controller_timestamp',  RAW_INDEX_INST_DCL_TIMESTAMP,    str),
    ('frame_header',              RAW_INDEX_INST_FRAME_HEADER,     str),
    ('frame_type',                RAW_INDEX_INST_FRAME_TYPE,       str),
    ('serial_number',             RAW_INDEX_INST_SERIAL_NUMBER,    str),
    ('date_of_sample',            RAW_INDEX_INST_DATE_OF_SAMPLE,   int),
    ('time_of_sample',            RAW_INDEX_INST_TIME_OF_SAMPLE,   float),
    ('nitrate_concentration',     RAW_INDEX_INST_NITRATE,          float),
    ('aux_fitting_1',             RAW_INDEX_INST_FITTING1,         float),
    ('aux_fitting_2',             RAW_INDEX_INST_FITTING2,         float),
    ('aux_fitting_3',             RAW_INDEX_INST_FITTING3,         float),
    ('rms_error',                 RAW_INDEX_INST_RMS_ERROR,        float)
]

# Indices into raw_data for Metadata particles.
RAW_INDEX_META_INTERNAL_TIMESTAMP = 0
RAW_INDEX_META_DCL_TIMESTAMP = 1
RAW_INDEX_META_SERIAL_NUMBER = 2
RAW_INDEX_META_STARTUP_TIME = 3
RAW_INDEX_META_SPEC_ON_TIME = 4
RAW_INDEX_META_SPEC_POWERED_TIME = 5
RAW_INDEX_META_LAMP_ON_TIME = 6
RAW_INDEX_META_LAMP_POWERED_TIME = 7
RAW_INDEX_META_LOG_FILE = 8

# This table is used in the generation of the metadata data particle.
# Column 1 - particle parameter name
# Column 2 - index into raw_data
# Column 3 - data encoding function (conversion required - int, float, etc)
METADATA_PARTICLE_MAP = [
    ('dcl_controller_timestamp',  RAW_INDEX_META_DCL_TIMESTAMP,      str),
    ('serial_number',             RAW_INDEX_META_SERIAL_NUMBER,      str),
    ('startup_time',              RAW_INDEX_META_STARTUP_TIME,       int),
    ('spec_on_time',              RAW_INDEX_META_SPEC_ON_TIME,       int),
    ('spec_powered_time',         RAW_INDEX_META_SPEC_POWERED_TIME,  int),
    ('lamp_on_time',              RAW_INDEX_META_LAMP_ON_TIME,       int),
    ('lamp_powered_time',         RAW_INDEX_META_LAMP_POWERED_TIME,  int),
    ('data_log_file',             RAW_INDEX_META_LOG_FILE,           str)
]


def calculate_unix_time(year, month, day, hour, minute, second):
    """
    This function calculates the number seconds since Jan 1, 1970 (Unix epoch).
    """
    return calendar.timegm((year, month, day, hour, minute, second))


class DataParticleType(BaseEnum):
    REC_INSTRUMENT_PARTICLE = 'nutnr_b_dcl_conc_instrument_recovered'
    REC_METADATA_PARTICLE = 'nutnr_b_dcl_conc_metadata_recovered'
    TEL_INSTRUMENT_PARTICLE = 'nutnr_b_dcl_conc_instrument'
    TEL_METADATA_PARTICLE = 'nutnr_b_dcl_conc_metadata'

    
class NutnrBDclConcInstrumentDataParticle(DataParticle):
    """
    Class for generating the Optaa_dj instrument particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(NutnrBDclConcInstrumentDataParticle, self).__init__(raw_data,
                                                          port_timestamp,
                                                          internal_timestamp,
                                                          preferred_timestamp,
                                                          quality_flag,
                                                          new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.

        timestamp = (
            int(self.raw_data[RAW_INDEX_INST_YEAR]),
            int(self.raw_data[RAW_INDEX_INST_MONTH]),
            int(self.raw_data[RAW_INDEX_INST_DAY]),
            int(self.raw_data[RAW_INDEX_INST_HOUR]),
            int(self.raw_data[RAW_INDEX_INST_MINUTE]),
            int(self.raw_data[RAW_INDEX_INST_SECOND]),
            0, 0, 0)
        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[raw_index], function)
            for name, raw_index, function in INSTRUMENT_PARTICLE_MAP]


class NutnrBDclConcRecoveredInstrumentDataParticle(NutnrBDclConcInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_INSTRUMENT_PARTICLE


class NutnrBDclConcTelemeteredInstrumentDataParticle(NutnrBDclConcInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_INSTRUMENT_PARTICLE


class NutnrBDclConcMetadataDataParticle(DataParticle):
    """
    Class for generating the Optaa_dj Metadata particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(NutnrBDclConcMetadataDataParticle, self).__init__(raw_data,
                                                          port_timestamp,
                                                          internal_timestamp,
                                                          preferred_timestamp,
                                                          quality_flag,
                                                          new_sequence)

        # The metadata particle timestamp has already been calculated by the parser.

        self.set_internal_timestamp(unix_time=self.raw_data[RAW_INDEX_META_INTERNAL_TIMESTAMP])

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Metadata Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Metadata Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[raw_index], function)
            for name, raw_index, function in METADATA_PARTICLE_MAP]


class NutnrBDclConcRecoveredMetadataDataParticle(NutnrBDclConcMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.REC_METADATA_PARTICLE


class NutnrBDclConcTelemeteredMetadataDataParticle(NutnrBDclConcMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TEL_METADATA_PARTICLE


class NutnrBDclConcParser(BufferLoadingParser):
    """
    Parser for Nutnr_b_dcl_conc data.
    In addition to the standard parser constructor parameters,
    this constructor needs the following additional parameters:
      instrument particle class
      metadata particle class.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 instrument_particle_class,
                 metadata_particle_class,
                 *args, **kwargs):

        super(NutnrBDclConcParser, self).__init__(config,
                                          stream_handle,
                                          state,
                                          partial(StringChunker.regex_sieve_function,
                                                  regex_list=[NUTNR_RECORD_MATCHER]),
                                          state_callback,
                                          publish_callback,
                                          exception_callback,
                                          *args,
                                          **kwargs)

        self.input_file = stream_handle
        self.metadata_state = 0
        self.metadata_timestamp = 0.0

        # Save the names of the particle classes to be generated.

        self.instrument_particle_class = instrument_particle_class
        self.metadata_particle_class = metadata_particle_class

    def check_for_new_block_metadata_record(self, chunk):
        """
        This function checks to see if this is metadata record indicating
        a new data block.
        """

        # Search for the Next Wakeup metadata record.
        # If found, that's the end of the current data block
        # and we prepare for another possible block.

        next_wakeup_match = NEXT_WAKEUP_MATCHER.match(chunk)
        if next_wakeup_match is not None:
            metadata_record = True
            self.metadata_state = 0
        else:
            metadata_record = False

        return metadata_record

    def check_for_idle_state_metadata_record(self, chunk):
        """
        This function checks to see if this is an Idle State metadata record.
        If it is, a timestamp to be used as the internal timestamp for the
        metadata particle is generated.
        """

        # If this is the Idle state metadata record,
        # calculate the metadata particle internal timestamp
        # from the DCL timestamp.

        idle_match = IDLE_TIME_MATCHER.match(chunk)
        if idle_match is not None:
            metadata_record = True
            self.metadata_timestamp = calculate_unix_time(
                int(idle_match.group(META_GROUP_DCL_YEAR)),
                int(idle_match.group(META_GROUP_DCL_MONTH)),
                int(idle_match.group(META_GROUP_DCL_DAY)),
                int(idle_match.group(META_GROUP_DCL_HOUR)),
                int(idle_match.group(META_GROUP_DCL_MINUTE)),
                int(idle_match.group(META_GROUP_DCL_SECOND))
            )

        else:
            metadata_record = False

        return metadata_record

    def check_for_instrument_record(self, chunk):
        """
        This function checks for an instrument data record.
        If found, a list of data particles is generated.
        """

        particle_list = []

        # See if it's an instrument data record.

        inst_match = INST_DATA_MATCHER.match(chunk)
        if inst_match is not None:

            # If the frame type is not DARK or LIGHT,
            # raise a recoverable sample exception.

            frame_type = inst_match.group(INST_GROUP_FRAME_TYPE)
            if frame_type != DARK and frame_type != LIGHT:
                error_message = 'nutnr_b_dcl_conc: Invalid frame type %s' % frame_type
                log.warn(error_message)
                self._exception_callback(RecoverableSampleException(error_message))

            else:

                # A frame type of DARK is the trigger for a metadata particle.
                # If all the metadata has been received,
                # generate the metadata particle for this block.

                if frame_type == DARK:
                    if self.metadata_state == ALL_METADATA_RECEIVED:

                        # Fields for the metadata particle must be
                        # in the same order as the RAW_INDEX_META_xxx values.
                        # DCL Controller timestamp and serial number
                        # are from the instrument data record.
                        # Other data comes from the various metadata records
                        # which has been accumulated in the Metadata State Table.

                        inst_fields = (
                            self.metadata_timestamp,
                            inst_match.group(INST_GROUP_DCL_TIMESTAMP),
                            inst_match.group(INST_GROUP_SERIAL_NUMBER)
                        )
                        meta_fields = [value
                            for state, matcher, value in METADATA_STATE_TABLE
                        ]
                        fields = inst_fields + tuple(meta_fields)

                        # Generate the metadata particle class and add the
                        # result to the list of particles to be returned.

                        particle = self._extract_sample(self.metadata_particle_class,
                                                        None,
                                                        fields,
                                                        None)

                        if particle is not None:
                            particle_list.append(particle)

                    else:
                        error_message = 'nutnr_b_dcl_conc: Incomplete Metadata'
                        log.warn(error_message)
                        self._exception_callback(RecoverableSampleException(error_message))

                if self.metadata_state == ALL_METADATA_RECEIVED:

                    # Fields for the instrument particle must be in the same
                    # order as the RAW_INDEX_INST_xxx values.

                    fields = (
                        inst_match.group(INST_GROUP_DCL_TIMESTAMP),
                        inst_match.group(INST_GROUP_FRAME_HEADER),
                        inst_match.group(INST_GROUP_FRAME_TYPE),
                        inst_match.group(INST_GROUP_SERIAL_NUMBER),
                        inst_match.group(INST_GROUP_JULIAN_DATE),
                        inst_match.group(INST_GROUP_TIME_OF_DAY),
                        inst_match.group(INST_GROUP_NITRATE),
                        inst_match.group(INST_GROUP_FITTING1),
                        inst_match.group(INST_GROUP_FITTING2),
                        inst_match.group(INST_GROUP_FITTING3),
                        inst_match.group(INST_GROUP_RMS_ERROR),
                        inst_match.group(INST_GROUP_DCL_YEAR),
                        inst_match.group(INST_GROUP_DCL_MONTH),
                        inst_match.group(INST_GROUP_DCL_DAY),
                        inst_match.group(INST_GROUP_DCL_HOUR),
                        inst_match.group(INST_GROUP_DCL_MINUTE),
                        inst_match.group(INST_GROUP_DCL_SECOND)
                    )

                    particle = self._extract_sample(self.instrument_particle_class,
                                                    None,
                                                    fields,
                                                    None)

                    if particle is not None:
                        particle_list.append(particle)

        return particle_list

    def check_for_metadata_record(self, chunk):
        """
        This function checks to see if a metadata record is contained
        in this chunk.
        Returns True or False
        """

        metadata_record = False

        # Search for one of the metadata records that contain data we need.

        meta_match = META_MESSAGE_MATCHER.match(chunk)

        if meta_match is not None:
            metadata_record = True

            for index, table_data in enumerate(METADATA_STATE_TABLE):
                state, matcher, value = table_data
                match = matcher.match(chunk)

                # If we get a match, it's one of the metadata records
                # that we're interested in.

                if match is not None:

                    # Update the state to reflect that we've got
                    # this particular metadata record.

                    self.metadata_state |= state

                    # For all matchers except the LOG_FILE matcher,
                    # convert the instrument time to seconds since
                    # Jan 1, 1970 (Unix Epoch time).

                    if matcher != LOG_FILE_MATCHER:
                        table_data[INDEX_VALUE] = calculate_unix_time(
                                int(match.group(META_GROUP_INST_YEAR)),
                                int(match.group(META_GROUP_INST_MONTH)),
                                int(match.group(META_GROUP_INST_DAY)),
                                int(match.group(META_GROUP_INST_HOUR)),
                                int(match.group(META_GROUP_INST_MINUTE)),
                                int(match.group(META_GROUP_INST_SECOND))
                        )

                    # For the LOG_FILE matcher, save the name of the log file.

                    else:
                        table_data[INDEX_VALUE] = match.group(META_GROUP_INST_LOGFILE)

        return metadata_record

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            self._exception_callback(UnexpectedDataException(
                "Found %d bytes of un-expected non-data %s" %
                (len(non_data), non_data)))

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this parsing.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:
            if self.check_for_idle_state_metadata_record(chunk):
                pass
            elif self.check_for_new_block_metadata_record(chunk):
                pass
            elif self.check_for_metadata_record(chunk):
                pass
            else:
                particle_list = self.check_for_instrument_record(chunk)
                for particle in particle_list:
                    result_particles.append((particle, None))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles
    

class NutnrBDclConcRecoveredParser(NutnrBDclConcParser):
    """
    This is the entry point for the Recovered Nutnr_b_dcl_conc parser.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        super(NutnrBDclConcRecoveredParser, self).__init__(config,
                                         stream_handle,
                                         state,
                                         state_callback,
                                         publish_callback,
                                         exception_callback,
                                         NutnrBDclConcRecoveredInstrumentDataParticle,
                                         NutnrBDclConcRecoveredMetadataDataParticle,
                                         *args,
                                         **kwargs)


class NutnrBDclConcTelemeteredParser(NutnrBDclConcParser):
    """
    This is the entry point for the Telemetered Nutnr_b_dcl_conc parser.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        super(NutnrBDclConcTelemeteredParser, self).__init__(config,
                                           stream_handle,
                                           state,
                                           state_callback,
                                           publish_callback,
                                           exception_callback,
                                           NutnrBDclConcTelemeteredInstrumentDataParticle,
                                           NutnrBDclConcTelemeteredMetadataDataParticle,
                                           *args,
                                           **kwargs)
