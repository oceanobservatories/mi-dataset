##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.log import get_logger
from mi.core.versioning import version
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.parad_k_stc_imodem import \
    Parad_k_stc_imodemParser

log = get_logger()


@version("0.0.4")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.parad_k_stc_imodem',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'Parad_k_stc_imodemDataParticle'
    }

    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()

    with open(sourceFilePath, 'rb') as stream_handle:
        parser = Parad_k_stc_imodemParser(parser_config,
                                          None,
                                          stream_handle,
                                          lambda state, ingested: None,
                                          lambda data: None,
                                          exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)

        driver.processFileStream()

    return particleDataHdlrObj
