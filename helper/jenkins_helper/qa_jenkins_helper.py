from helper.jenkins_helper.jenkins_helper import JenkinsHelper, SingeltonMetaClass
import os

class QAJenkinsHelper(JenkinsHelper, metaclass=SingeltonMetaClass):

    def __init__(self):
        qa_jenkins_url = os.environ.get("QA_JENKINS_URL")
        qa_jenkins_username = os.environ.get("QA_JENKINS_USERNAME")
        qa_jenkins_password = os.environ.get("QA_JENKINS_PASSWORD")
        super().__init__(url=qa_jenkins_url,
                         username=qa_jenkins_username,
                         password=qa_jenkins_password)