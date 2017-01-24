#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2017 Raytheon Co.
##
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.glider import GliderParser

__author__ = 'Rene Gelinas'


class FlortODriver:

    def __init__(self, source_file_path, particle_data_hdlr_obj, config):

        self._sourceFilePath = source_file_path
        self._particleDataHdlrObj = particle_data_hdlr_obj
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
