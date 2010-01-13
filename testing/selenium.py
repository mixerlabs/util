from selenium_rc.selenium import selenium
import unittest
import atexit
import os
import socket
import time
from environment import env
from subprocess import Popen

global_selenium = None
failure = False;

def start_server():
    global global_selenium
    global_selenium = selenium("localhost", 4444, "*firefox", "http://localhost:8080/hawaii")
    global_selenium.start()

def stop_if_no_errors():
    if not failure:
        global_selenium.stop()

try:
    start_server()
except socket.error:
    # try starting up the java server
    cmd = ["java", "-jar", env.root + "/ext/selenium-server/selenium-server.jar" ]
    profile_template = os.getenv("SELENIUM_PROFILE_TEMPLATE", None)
    if profile_template:
        cmd.extend(["-firefoxProfileTemplate", profile_template])
    print "cmd: " + str(cmd)
    Popen(cmd)
    time.sleep(1)
    start_server()

atexit.register(stop_if_no_errors)

class ResultListenerProxy:
    def __init__(self, results):
        self.results = results

    def addFailure(self, *args, **kwargs):
        global failure
        failure = True
        self.results.addFailure(*args, **kwargs)

    def addError(self, *args, **kwargs):
        global failure
        failure = True
        self.results.addError(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self.results, attr)

class SeleniumTestCase(unittest.TestCase):
    def run(self, result=None):
        if result is None: result = self.defaultTestResult();
        if not isinstance(result, ResultListenerProxy):
            result = ResultListenerProxy(result)
        unittest.TestCase.run(self, result)

    def __init__(self, methodName="runTest"):
        unittest.TestCase.__init__(self, methodName)


