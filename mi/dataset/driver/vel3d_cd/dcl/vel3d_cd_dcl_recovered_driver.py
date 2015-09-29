#!/usr/bin/env python

"""
@package mi.dataset.driver.vel3d_cd.dcl
@file mi/dataset/driver/vel3d_cd/dcl/vel3d_cd_dcl_recovered_driver.py
@author Emily Hahn
@brief Driver for the recovered vel3d instrument series c and d through dcl
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.vel3d_cd_dcl import Vel3dCdDclParser
from mi.core.versioning import version


@version("15.7.0")
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
        driver = Vel3dCdDclRecoveredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class Vel3dCdDclRecoveredDriver(SimpleDatasetDriver):
    """
    Create a _build_parser method for building the vel3d cd dcl parser
    """
    def _build_parser(self, stream_handle):
        """
        Build the vel3d cd dcl parser
        :param stream_handle: The file handle to pass into the parser
        :return: The created parser class
        """
        # no config input, set is telemetered flag to false to indicate recovered
        return Vel3dCdDclParser(stream_handle, self._exception_callback, is_telemetered=False)
