#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.dataset.driver.metbk_a.dcl.metbk_dcl_a_driver import process, \
    TELEMETERED_PARTICLE_CLASS
from mi.core.versioning import version


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    process(sourceFilePath, particleDataHdlrObj, TELEMETERED_PARTICLE_CLASS)

    return particleDataHdlrObj
