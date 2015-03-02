#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpa_n
@file marine-integrations/mi/dataset/parser/adcpa_n.py
@author Jeff Roy
@brief Particle classes for the adcpa_n dataset driver
These particles are parsed by the common PD0 Parser and
Abstract particle class in file adcp_pd0.py
Release notes:

initial release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue

from mi.dataset.parser.adcp_pd0 import AdcpFileType, AdcpPd0DataParticle


class AdcpaNInstrumentParticle(AdcpPd0DataParticle):

    # set the data_particle_type for the DataParticle class
    #  note: this must be done outside of the constructor because
    # _data_particle_type is used by the class method type(cls)
    # in the DataParticle class
    _data_particle_type = "adcpa_n_instrument"

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        # set the file type for use by the AdcpPd0DataParticle class
        file_type = AdcpFileType.ADCPA_AUV_FILE
        # file_type must be set to a value in AdcpFileType

        super(AdcpaNInstrumentParticle, self).__init__(raw_data,
                                                       port_timestamp,
                                                       internal_timestamp,
                                                       preferred_timestamp,
                                                       quality_flag,
                                                       new_sequence,
                                                       file_type)


