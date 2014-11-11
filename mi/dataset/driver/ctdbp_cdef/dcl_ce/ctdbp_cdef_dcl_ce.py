#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_cdef.dcl_ce.ctdbp_cdef_dcl_ce
@file mi-dataset/mi/dataset/driver/ctdbp_cdef/dcl_ce/ctdbp_cdef_dcl_ce.py
@author Christopher Fortin
@brief Driver for the ctdbp_cdef_dcl_ce instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.core.exceptions import NotImplementedException
from mi.dataset.parser.ctdbp_cdef_dcl_ce import CtdbpCdefDclCeParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rb') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = CtdbpCdefDclCe(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class CtdbpCdefDclCe(SimpleDatasetDriver):
    """
    Derived ctdbp_cdef_dcl_ce driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        raise NotImplementedException("_build_parser() not overridden!")