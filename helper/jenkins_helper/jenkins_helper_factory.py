import logging
from helper.jenkins_helper.qe_jenkins_helper import QEJenkinsHelper
from helper.jenkins_helper.qa_jenkins_helper import QAJenkinsHelper

logger = logging.getLogger("helper")

class JenkinsHelperFactory:

    QA_JENKINS = "qa_jenkins"
    QE_JENKINS = "qe_jenkins"

    @staticmethod
    def fetch_helper(name):
        if name == JenkinsHelperFactory.QA_JENKINS:
            return QAJenkinsHelper()
        elif name == JenkinsHelperFactory.QE_JENKINS:
            return QEJenkinsHelper()
        else:
            error_msg = "Illegal jenkins host name"
            logger.error(error_msg)
            raise ValueError(error_msg)
