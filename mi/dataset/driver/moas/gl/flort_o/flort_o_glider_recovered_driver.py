#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2017 Raytheon Co.
##
import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.moas.gl.flort_o.flort_o_glider_driver import FlortODriver
from mi.core.versioning import version

__author__ = 'Rene Gelinas'


@version("1.0.0")
def parse(base_python_code_path, source_file_path, particle_data_hdlr_obj):

    config.add_configuration(os.path.join(base_python_code_path, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortORecoveredDataParticle',
    }

    driver = FlortODriver(base_python_code_path, source_file_path, particle_data_hdlr_obj, parser_config)
    return driver.process()
