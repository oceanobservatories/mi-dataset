#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Jeff Roy'

import os

from mi.logging import config

from mi.dataset.driver.presf_abc.dcl.presf_abc_dcl_common_driver import PresfAbcDclDriver
from mi.dataset.parser.presf_abc_dcl import DataTypeKey


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    driver = PresfAbcDclDriver(sourceFilePath, particleDataHdlrObj, DataTypeKey.PRESF_ABC_DCL_RECOVERED)

    return driver.process()