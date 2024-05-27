QA_JENKINS = "qa_jenkins"
QE_JENKINS = "qe_jenkins"
JENKINS_URLS = {
    "http://qa.sc.couchbase.com" : QA_JENKINS,
    "http://qe-jenkins1.sc.couchbase.com" : QE_JENKINS
}
JENKINS_SSH_LAUNCHER_CREDS = {
    "http://qa.sc.couchbase.com" : {
        "root:couchbase" : "root"
    },
    "http://qe-jenkins1.sc.couchbase.com" : {
        "root:couchbase" : "allslavecreds"
    }
}