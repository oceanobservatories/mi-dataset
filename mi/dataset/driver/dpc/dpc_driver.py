#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##


from mi.dataset.parser.dpc import DeepProfilerParticle
from mi.core.log import get_logger
from mi.logging import config
import os

log = get_logger()

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
    for particle in DeepProfilerParticle.process_msgpack(sourceFilePath):
        if particle is not None:
            particleDataHdlrObj.addParticleSample(particle._data_particle_type, particle.generate())
        else:
            particleDataHdlrObj.setParticleDataCaptureFailure()

    return particleDataHdlrObj