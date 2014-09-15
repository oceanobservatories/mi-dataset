__author__ = 'wordenm'

from mi.core.log import get_logger
log = get_logger()

# The following class is a stub class.  The real class is a Java class.
# This script is expected to be used via JEP (Java Embedded Python)
class ParticleDataHandler(object):

    def __init__(self):
        pass

    def addParticleSample(self, sample_type, sample):
        log.debug("Sample type: %s, Sample data: %s", sample_type, sample)

    def setParticleDataCaptureFailure(self):
        log.debug("Particle data capture failed")


class DataSetDriver:

    def __init__(self, parser, particleDataHdlrObj):

        self._parser = parser
        self._particleDataHdlrObj = particleDataHdlrObj

    def processFileStream(self):
        while True:
            try:
                records = self._parser.get_records(1)

                if len(records) == 0:
                    log.debug("Done retrieving records.")
                    break

                for record in records:
                    log.debug("Adding record %s", record.generate())
                    self._particleDataHdlrObj.addParticleSample(record.type(), record.generate())
            except Exception as e:
                log.debug(e)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()
                break
