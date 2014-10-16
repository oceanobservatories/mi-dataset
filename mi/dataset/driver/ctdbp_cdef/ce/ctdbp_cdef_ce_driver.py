#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_cdef.ce
@file marine-integrations/mi/dataset/driver/ctdbp_cdef/ce/ctdbp_cdef_ce.py
@author Tapana Gupta
@brief Driver for the ctdbp_cdef_ce instrument

Release notes:

Initial Release
"""

from mi.core.log import get_logger

from mi.dataset.parser.ctdbp_cdef_ce import CtdbpCdefCeParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    log = get_logger()

    with open(sourceFilePath, "r") as stream_handle:

        def exception_callback(exception):
                log.debug("Exception: %s", exception)
                particleDataHdlrObj.setParticleDataCaptureFailure()

        parser = CtdbpCdefCeParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.ctdbp_cdef_ce",
             DataSetDriverConfigKeys.PARTICLE_CLASS: None},
             stream_handle,
             lambda state, ingested: None,
             lambda data: None,
             exception_callback
        )
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()
    return particleDataHdlrObj
