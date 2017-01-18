#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Rachel Manoni'

import sys

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.glider import GliderParser


class FlordMDriver:
    def __init__(self, unused, sourceFilePath, particleDataHdlrObj, config):

        self._unused = unused
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._config = config

    def process(self):

        with open(self._sourceFilePath, 'rb') as stream_handle:
            def exp_callback(exception):
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = GliderParser(self._config,
                                  stream_handle,
                                  exp_callback)
            driver = DataSetDriver(parser, self._particleDataHdlrObj)
            driver.processFileStream()

        return self._particleDataHdlrObj
