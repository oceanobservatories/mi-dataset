#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_cg_dcl_eng_dcl
@file mi/dataset/parser/test/test_cg_dcl_eng_dcl.py
@author Mark Worden
@brief Test code for a cg_dcl_eng_dcl data parser
"""
import os

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import SampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cg_dcl_eng_dcl import CgDclEngDclParser, CgDclEngDclParticleClassTypes, \
    CgDclEngDclMsgCountsRecoveredDataParticle, CgDclEngDclMsgCountsTelemeteredDataParticle, \
    CgDclEngDclCpuUptimeRecoveredDataParticle, CgDclEngDclCpuUptimeTelemeteredDataParticle, \
    CgDclEngDclErrorRecoveredDataParticle, CgDclEngDclErrorTelemeteredDataParticle, \
    CgDclEngDclGpsRecoveredDataParticle, CgDclEngDclGpsTelemeteredDataParticle, \
    CgDclEngDclPpsRecoveredDataParticle, CgDclEngDclPpsTelemeteredDataParticle, \
    CgDclEngDclSupervRecoveredDataParticle, CgDclEngDclSupervTelemeteredDataParticle, \
    CgDclEngDclDlogMgrRecoveredDataParticle, CgDclEngDclDlogMgrTelemeteredDataParticle, \
    CgDclEngDclDlogStatusRecoveredDataParticle, CgDclEngDclDlogStatusTelemeteredDataParticle, \
    CgDclEngDclStatusRecoveredDataParticle, CgDclEngDclStatusTelemeteredDataParticle, \
    CgDclEngDclDlogAarmRecoveredDataParticle, CgDclEngDclDlogAarmTelemeteredDataParticle

from mi.idk.config import Config
RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                             'dataset', 'driver', 'cg_dcl_eng',
                             'dcl', 'resource')

@attr('UNIT', group='mi')
class CgParserUnitTestCase(ParserUnitTestCase):
    """
    Cg_stc_eng_stc Parser unit test suite
    """
    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self._exceptions_detected = 0

        self._recovered_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                CgDclEngDclParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.ERROR_PARTICLE_CLASS:
                    CgDclEngDclErrorRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS:
                    CgDclEngDclDlogStatusRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusRecoveredDataParticle,
                CgDclEngDclParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmRecoveredDataParticle,
            }
        }

        self._telemetered_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                CgDclEngDclParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.ERROR_PARTICLE_CLASS:
                    CgDclEngDclErrorTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS:
                    CgDclEngDclDlogStatusTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusTelemeteredDataParticle,
                CgDclEngDclParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmTelemeteredDataParticle,
            }
        }

    def exception_callback(self, exception):
        log.debug("Exception received: %s", exception)
        self._exceptions_detected += 1

    def test_happy_path(self):
        """
        Read files and verify that all expected particles can be read.
        Verify that the contents of the particles are correct.
        There should be no exceptions generated in processing the input files.
        """
        log.debug('===== START TEST HAPPY PATH =====')

        with open(os.path.join(RESOURCE_PATH, 'DCL26_20131220.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(100)

            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)

            self.assert_particles(particles, 'recov.DCL26_20131220.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140915.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(100)

            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)

            self.assert_particles(particles, 'recov.20140915.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, 'DCL26_20131220.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(100)

            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)

            self.assert_particles(particles, 'telem.DCL26_20131220.syslog.yml', RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140915.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(100)

            log.debug("particles: %s", particles)

            self.assertEqual(self._exceptions_detected, 0)

            self.assert_particles(particles, 'telem.20140915.syslog.yml', RESOURCE_PATH)

        log.debug('===== END TEST HAPPY PATH =====')

    def test_invalid_fields(self):
        """
        This text method will ensure that invalid file records are found and reported.
        """
        log.debug('===== START TEST INVALID FIELDS =====')

        with open(os.path.join(RESOURCE_PATH, 'invalid.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(50)

            log.debug("Result: %s", particles)

            self.assertEqual(len(particles), 13)

            self.assertEqual(self._exceptions_detected, 35)

            self.assert_particles(particles, 'recov.invalid.syslog.yml', RESOURCE_PATH)

        self._exceptions_detected = 0

        with open(os.path.join(RESOURCE_PATH, 'invalid.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(50)

            log.debug("Result: %s", particles)

            self.assertEqual(len(particles), 13)

            self.assertEqual(self._exceptions_detected, 35)

            self.assert_particles(particles, 'telem.invalid.syslog.yml', RESOURCE_PATH)

        log.debug('===== END TEST INVALID FIELDS =====')

    def test_no_particles(self):
        """
        Verify that no particles are produced if the input file has no valid records.
        """
        log.debug('===== START TEST NO PARTICLES =====')

        with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._recovered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(1)

            log.debug("Result: %s", len(particles))

            self.assertEqual(len(particles), 0)

            self.assertEqual(self._exceptions_detected, 0)

        with open(os.path.join(RESOURCE_PATH, 'no_particles.syslog.log')) as file_handle:
            self.parser = CgDclEngDclParser(self._telemetered_config, file_handle,
                                            self.exception_callback)

            particles = self.parser.get_records(1)

            log.debug("Result: %s", len(particles))

            self.assertEqual(len(particles), 0)

            self.assertEqual(self._exceptions_detected, 0)

        log.debug('===== END TEST NO PARTICLES =====')
