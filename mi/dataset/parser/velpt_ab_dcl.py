#!/usr/bin/env python

"""
@package mi.dataset.parser
@file /mi/dataset/parser/velpt_ab_dcl.py
@author Chris Goodrich
@brief Parser for the velpt_ab_dcl recovered and telemetered dataset driver
Release notes:

initial release
"""
__author__ = 'Chris Goodrich'
__license__ = 'Apache 2.0'

import struct

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.parser.velpt_ab_dcl_particles import VelptAbDataParticle
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.common import BaseEnum
from mi.core.exceptions import ConfigurationException

"""
Sample Aquadopp Velocity Data Record (42 bytes)
A5 01 1500 15 16 13 00 14 08 0000 0000 7500 8C3B AC07 F4FF EEFF 00 11 F500 3E08 1B00 E5FF E9FF 8E 87 71 00 2CBB
---------------------------------------------------------------------------------------------------------------
Data Mapping:
Read                          Swapped     Name                Offset
A5      -> Sync               A5
01      -> Id                 01
1500    -> Record size        0015
15      -> Minute             15
16      -> Second             16
13      -> Day                13
00      -> Hour               00
14      -> Year               14
08      -> Month              08
0000    -> Error              0000        error_code          10
0000    -> AnaIn1             0000        analog1             12
7500    -> Battery            0075        battery_voltage     14
8C3B    -> SoundSpeed/AnaIn2  3B8C        sound_speed_analog2 16
AC07    -> Heading            07AC        heading             18
F4FF    -> Pitch              FFF4        pitch               20
EEFF    -> Roll               FFEE        roll                22
00      -> PressureMSB        00                              24
11      -> Status             11          status              25
F500    -> PressureLSW        00F5        pressure            26
3E08    -> Temperature        083E        temperature         28
1B00    -> Vel B1/X/E         001B        velocity_beam1      30
E5FF    -> Vel B2/Y/N         FFE5        velocity_beam2      32
E9FF    -> Vel B3/Z/U         FFE9        velocity_beam3      34
8E      -> Amp B1             8E          amplitude_beam1     36
87      -> Amp B2             87          amplitude_beam2     37
71      -> Amp B3             71          amplitude_beam3     38
00      -> Fill               00
2CBB    -> Checksum           BB2C

Sample Diagnostics Header Record (36 bytes)
A5 06 1200 1400 0100 00 00 00 00 2016 1300 1408 0000 0000 0000 0000 0000 000000000000 9FDA
------------------------------------------------------------------------------------------
Data Mapping:
Read                            Swapped         Name                        Offset
A5              -> Sync         A5
60              -> Id           60
1200            -> Record size  0012
1400            -> Records      0014            records_to_follow           4
0100            -> Cell         0001            cell_number_diagnostics     6
00              -> Noise1       00              noise_amplitude_beam1       8
00              -> Noise2       00              noise_amplitude_beam2       9
00              -> Noise3       00              noise_amplitude_beam3       10
00              -> Noise4       00              noise_amplitude_beam4       11
2016            -> ProcMagn1    1620            processing_magnitude_beam1  12
1300            -> ProcMagn2    0013            processing_magnitude_beam2  14
1408            -> ProcMagn3    0814            processing_magnitude_beam3  16
0000            -> ProcMagn4    0000            processing_magnitude_beam4  18
0000            -> Distance1    0000            distance_beam1              20
0000            -> Distance2    0000            distance_beam2              22
0000            -> Distance3    0000            distance_beam3              23
0000            -> Distance4    0000            distance_beam4              26
000000000000    -> Spare        000000000000
9FDA            -> Checksum     DA9F

Sample Diagnostics Data Record (42 bytes)
A5 80 1500 20 17 13 00 14 08 0000 0000 7500 8C3B A307 F3FF F1FF 00 11 EF00 3B08 67FF C8FA D916 35 33 35 00 B1F7
---------------------------------------------------------------------------------------------------------------
Data Mapping:
Read                            Swapped     Name                Offset
A5      -> Sync                 A5
80      -> Id                   80
1500    -> Record size          0015
20      -> Minute               15
17      -> Second               16
13      -> Day                  13
00      -> Hour                 00
14      -> Year                 14
08      -> Month                08
0000    -> Error                0000        error_code          10
0000    -> AnaIn1               0000        analog1             12
7500    -> Battery              0075        battery_voltage     14
8C3B    -> SoundSpeed/AnaIn2    3B8C        sound_speed_analog2 16
A307    -> Heading              07A3        heading             18
F3FF    -> Pitch                FFF3        pitch               20
F1FF    -> Roll                 FFF1        roll                22
00      -> PressureMSB          00                              24
11      -> Status               11          status              25
EF00    -> PressureLSW          00EF        pressure            26
3B08    -> Temperature          083B        temperature         28
67FF    -> Vel B1/X/E           FF67        velocity_beam1      30
C8FA    -> Vel B2/Y/N           FAC8        velocity_beam2      32
D916    -> Vel B3/Z/U           16D9        velocity_beam3      34
35      -> Amp B1               35          amplitude_beam1     36
33      -> Amp B2               33          amplitude_beam2     37
35      -> Amp B3               35          amplitude_beam3     38
00      -> Fill                 00
B1F7    -> Checksum             F7B1

"""


class VelptAbParticleClassKey (BaseEnum):
    """
    An enum for the keys application to the pco2w abc particle classes
    """
    METADATA_PARTICLE_CLASS = 'metadata_particle_class'
    DIAGNOSTICS_PARTICLE_CLASS = 'diagnostics_particle_class'
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle_class'


class VelptAbDclParser(SimpleParser):
    """
    Class used to parse the velpt_ab_dcl data set.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 state_callback=None,
                 publish_callback=None):

        self._record_buffer = []
        self._calculated_checksum = 0

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)

            # Set the metadata and data particle classes to be used later

            if VelptAbParticleClassKey.METADATA_PARTICLE_CLASS in particle_classes_dict and \
               VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS in particle_classes_dict and \
               VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS in particle_classes_dict:

                self._metadata_class = config[
                    DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][VelptAbParticleClassKey.METADATA_PARTICLE_CLASS]
                self._diagnostics_class = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS]
                self._velocity_data_class = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS]
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
        else:
            log.warning('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

        super(VelptAbDclParser, self).__init__(config, stream_handle, exception_callback)

    @staticmethod
    def _checksums_match(self, record, length, checksum):
        """
        Determines if the stored checksum matches the actual checksum
        :param record: the read-in record
        :record_checksum: the checksum from the record
        :return: boolean
        """
        # 46476 is the base value of the checksum given in the IDD as 0xB58C
        self._calculated_checksum = 46476

        for x in range(0, length-3, 2):
            self._calculated_checksum += struct.unpack('<H', record[x:x+2])[0]

        # Modulo 65536 is applied to the checksum to keep it a 16 bit value
        self._calculated_checksum %= 65536

        if self._calculated_checksum == checksum:
            return True
        else:
            return False

    def parse_file(self):
        """
        Parser for velpt_ab_dcl data.
        """
        sync_marker = b'\xA5'
        velocity_data_id = b'\x01'
        diagnostic_header_id = b'\x06'
        diagnostic_data_id = b'\x80'
        done = False
        record_start = 0
        record_length = 0
        first_diagnostics_header_record = True
        diagnostics_count = 0
        total_diagnostic_records = 0
        velocity_data_dict = {}
        diagnostics_header_dict = {}
        diagnostics_data_dict = {}
        diagnostics_header_record = ''
        sending_diagnostics = False
        bad_byte = False

        while not done:

            # Assume the record will be good
            good_record = True

            # Reset the record type flags
            velocity_data = False
            diagnostic_header = False
            diagnostic_data = False

            # LOSE THIS!
            if bad_byte:
                bad_byte = False

            # Read the Sync byte
            sync_byte = self._stream_handle.read(1)

            # If the first byte is the sync byte (0xA5)
            # read the ID byte and the record length.
            if sync_byte == sync_marker:

                # Capture the record start address for reading the entire record later
                record_start = self._stream_handle.tell() - 1

                # Assume the id byte is good
                good_id = True

                # Read the ID byte
                id_byte = self._stream_handle.read(1)

                # If this a valid record type (0x01 | 0x06 | 0x80)
                # continue processing.
                if id_byte == velocity_data_id:
                    velocity_data = True
                elif id_byte == diagnostic_header_id:
                    diagnostic_header = True
                elif id_byte == diagnostic_data_id:
                    diagnostic_data = True
                else:
                    good_id = False
                    good_record = False

                if good_id:
                    record_length = struct.unpack('<H', self._stream_handle.read(2))[0]*2
                    bad_byte = False
                else:
                    bad_byte = True
                    log.warning('Found invalid ID byte: %d, at %d skipping to next sync byte',
                                struct.unpack('B', id_byte)[0], self._stream_handle.tell()-1)
                    self._exception_callback(
                        RecoverableSampleException('Found Invalid ID Byte, skipping to next record'))

            else:
                if sync_byte == '':
                    done = True
                else:
                    bad_byte = True
                    good_record = False
                    log.warning('Found invalid sync byte: %d at %d , skipping to next byte',
                                struct.unpack('B', sync_byte)[0], self._stream_handle.tell()-1)
                    self._exception_callback(
                        RecoverableSampleException('Found Invalid Sync Byte, skipping to next record'))

            # If the record is valid, read it
            if good_record and not done:

                self._stream_handle.seek(record_start, 0)

                current_record = self._stream_handle.read(record_length)

                if len(current_record) == record_length:

                    record_start += record_length

                    stored_checksum = struct.unpack('<H', current_record[(record_length-2):record_length])[0]

                    if self._checksums_match(self, current_record, record_length, stored_checksum):

                        if velocity_data:

                            if sending_diagnostics:
                                sending_diagnostics = False
                                if total_diagnostic_records != diagnostics_count:
                                    if diagnostics_count < total_diagnostic_records:
                                        log.warning('Not enough diagnostics records, got %s, expected %d',
                                                    diagnostics_count, total_diagnostic_records)
                                        self._exception_callback(
                                            RecoverableSampleException('Not enough diagnostics records'))

                                    elif diagnostics_count > total_diagnostic_records:
                                        log.warning('Too many diagnostics records, got %s, expected %d',
                                                    diagnostics_count, total_diagnostic_records)
                                        self._exception_callback(
                                            RecoverableSampleException('Too many diagnostics records'))
                                diagnostics_count = 0
                                total_diagnostic_records = 0

                            timestamp = VelptAbDataParticle.get_timestamp(current_record)

                            velocity_data_dict = VelptAbDataParticle.generate_data_dict(self, current_record)

                            particle = self._extract_sample(self._velocity_data_class,
                                                            None,
                                                            velocity_data_dict,
                                                            timestamp)

                            self._record_buffer.append(particle)

                        elif diagnostic_data:

                            # As diagnostics records have the same format as velocity records
                            # you can use the same routine used to break down the velocity data

                            timestamp = VelptAbDataParticle.get_timestamp(current_record)
                            date_time_group = VelptAbDataParticle.get_date_time_string(current_record)

                            diagnostics_data_dict = VelptAbDataParticle.generate_data_dict(self, current_record)

                            if first_diagnostics_header_record:
                                first_diagnostics_header_record = False

                                diagnostics_header_dict =\
                                    VelptAbDataParticle.generate_diagnostics_header_dict(self, date_time_group,
                                                                                         diagnostics_header_record)

                                total_diagnostic_records = VelptAbDataParticle.get_diagnostics_count(
                                    diagnostics_header_record)

                                particle = self._extract_sample(self._metadata_class,
                                                                None,
                                                                diagnostics_header_dict,
                                                                timestamp)

                                self._record_buffer.append(particle)

                            particle = self._extract_sample(self._diagnostics_class,
                                                            None,
                                                            diagnostics_data_dict,
                                                            timestamp)

                            self._record_buffer.append(particle)

                            diagnostics_count += 1

                        elif diagnostic_header:
                            first_diagnostics_header_record = True
                            diagnostics_count = 0
                            diagnostics_header_record = current_record
                            sending_diagnostics = True

                    else:
                        log.warning('Invalid checksum: %d, expected %d - record will not be processed',
                                    stored_checksum, self._calculated_checksum)
                        self._exception_callback(
                            RecoverableSampleException('Invalid checksum, no particle generated'))

                else:
                    done = True
                    log.warning('Last record in file was malformed')
                    self._exception_callback(
                        RecoverableSampleException('Last record in file malformed, no particle generated'))

        log.trace('File has been completely processed')
