#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2016 Raytheon Co.
##
__author__ = 'Rene Gelinas'

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.presf_abc import PresfAbcParser
from mi.core.versioning import version


@version("1.0.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = PresfAbcRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class PresfAbcRecoveredDriver(SimpleDatasetDriver):
    """
    Derived presf_abc_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = PresfAbcParser(stream_handle, self._exception_callback)

        return parser


