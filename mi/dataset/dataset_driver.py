__author__ = 'wordenm'

from mi.core.log import get_logger
log = get_logger()


# The following class is a stub class.  The real class is a Java class.
# This script is expected to be used via JEP (Java Embedded Python).
# This stub class is needed for driver level testing without uFrame.
class ParticleDataHandler(object):

    def __init__(self):
        self._samples = {}
        self._failure = False

    def addParticleSample(self, sample_type, sample):
        log.debug("Sample type: %s, Sample data: %s", sample_type, sample)
        if sample_type not in self._samples.keys():
            self._samples[sample_type] = [sample]
        else:
            self._samples[sample_type].append(sample)

    def setParticleDataCaptureFailure(self):
        log.debug("Particle data capture failed")
        self._failure = True


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
