from helper.jenkins_helper.jenkins_helper import JenkinsHelper, SingeltonMetaClass
import os

class QEJenkinsHelper(JenkinsHelper, metaclass=SingeltonMetaClass):

    def __init__(self):
        qe_jenkins_url = os.environ.get("QE_JENKINS_URL")
        qe_jenkins_username = os.environ.get("QE_JENKINS_USERNAME")
        qe_jenkins_password = os.environ.get("QE_JENKINS_PASSWORD")
        super().__init__(url=qe_jenkins_url,
                         username=qe_jenkins_username,
                         password=qe_jenkins_password)