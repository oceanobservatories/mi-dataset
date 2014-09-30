"""
@package mi.dataset.driver.presf_abc.dcl.driver
@file marine-integrations/mi/dataset/driver/presf_abc/dcl/driver.py
@author Christopher Fortin
@brief Driver for the presf_abc_dcl
Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import string

from mi.core.log import get_logger ; log = get_logger()

from mi.dataset.dataset_driver import SimpleDataSetDriver
from mi.dataset.parser.presf_abc_dcl import PresfAbcDclParser, PresfAbcDclParserDataParticle

class PresfAbcDclDataSetDriver(SimpleDataSetDriver):
    
    @classmethod
    def stream_config(cls):
        return [PresfAbcDclParserDataParticle.type()]

    def _build_parser(self, parser_state, infile):
        """
        Build and return the parser
        """
        config = self._parser_config
        config.update({
            'particle_module': 'mi.dataset.parser.presf_abc_dcl',
            'particle_class': 'PresfAbcDclParserDataParticle'
        })
        log.debug("My Config: %s", config)
        self._parser = PresfAbcDclParser(
            config,
            parser_state,
            infile,
            self._save_parser_state,
            self._data_callback,
            self._sample_exception_callback 
        )
        return self._parser

    def _build_harvester(self, driver_state):
        """
        Build and return the harvester
        """
        # *** Replace the following with harvester initialization ***
        self._harvester = None     
        return self._harvester
