__author__ = 'wordenm'

import os

from mi.logging import config
from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import NotImplementedException

class ParticleDataHandler(object):
    """
    This class is a stub class.  The real class is a Java class.
    This script is expected to be used via JEP (Java Embedded Python).
    This stub class is needed for driver level testing without uFrame.
    """

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


class DataSetDriver(object):
    """
    Base Class for dataset drivers used within uFrame
    This class of objects processFileStream method
    will be used by the parse method
    which is called directly from uFrame
    """

    def __init__(self, parser, particleDataHdlrObj):

        self._parser = parser
        self._particleDataHdlrObj = particleDataHdlrObj

    def processFileStream(self):
        """
        Method to extract records from a parser's get_records method
        and pass them to the Java particleDataHdlrObj passed in from uFrame
        """
        while True:
            try:
                records = self._parser.get_records(1)

                if len(records) == 0:
                    log.debug("Done retrieving records.")
                    break

                for record in records:
                    self._particleDataHdlrObj.addParticleSample(record.type(), record.generate())
            except Exception as e:
                log.error(e)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()
                break


class SimpleDatasetDriver(DataSetDriver):
    """
    Abstract class to simplify driver writing.  Derived classes simply need to provide
    the _build_parser method
    """

    def __init__(self, basePythonCodePath, stream_handle, particleDataHdlrObj):

        #configure the mi logger
        config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))
        parser = self._build_parser(stream_handle)

        super(SimpleDatasetDriver, self).__init__(parser, particleDataHdlrObj)

    def _build_parser(self, stream_handle):
        """
        abstract method that must be provided by derived classes to build a parser
        :param stream_handle: an open fid created from the sourceFilePath passed in from edex
        :return: A properly configured parser object
        """

        raise NotImplementedException("_build_parser must be implemented")

    def _exception_callback(self, exception):
        """
        A common exception callback method that can be used by _build_parser methods to
        map any exceptions coming from the parser back to the edex particleDataHdlrObj
        :param exception: any exception from the parser
        :return: None
        """

        log.debug("ERROR: %r", exception)
        self._particleDataHdlrObj.setParticleDataCaptureFailure()

