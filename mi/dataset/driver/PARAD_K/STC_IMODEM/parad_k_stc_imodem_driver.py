#!/usr/bin/env python

import sys

from mi.core.log import get_logger
from mi.core.versioning import version
from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.parad_k_stc_imodem import Parad_k_stc_imodemParser

log = get_logger()


@version("0.0.4")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_k_stc_imodem',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'Parad_k_stc_imodemDataParticle'
    }

    def state_callback(state, ingested):
        pass

    def pub_callback(data):
        log.trace("Found data: %s", data)

    def exception_callback(exception):
        particleDataHdlrObj.setParticleDataCaptureFailure()

    stream_handle = open(sourceFilePath, 'rb')

    try:
        parser = Parad_k_stc_imodemParser(config, None, stream_handle,
                                          state_callback, pub_callback,
                                          exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)

        driver.processFileStream()

    finally:
        stream_handle.close()

    return particleDataHdlrObj
