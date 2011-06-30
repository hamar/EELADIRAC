########################################################################
# $Id: JSAGAComputingElement.py 18161 2009-11-11 12:07:09Z acasajus $
# File :   JSAGAComputingElement.py
# Author : Vanessa Hamar
########################################################################

""" The simplest Computing Element instance that submits jobs using JSAGA commands.
    _addCEConfigDefaults
    submitJob(executableFile, proxy, numberOfPilots=1) -> (pilotList)
    getJobStatus(pilotList) -> {pilotsStatus}
    getJobOutput(pilotID,proxy.dir=None) -> {StdOut,StdError}

"""

__RCSID__ = ""

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK,S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.Misc                            import getProxyInfo
from DIRAC.Core.Utilities.Grid                           import executeGridCommand

import os,sys, time, re, socket
import string, shutil, bz2, base64, tempfile

CE_NAME = 'JSAGA'


class JSAGAComputingElement( ComputingElement ):


  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0
    self.gridEnv     = gConfig.getValue(self.section+'/JSAGAEnv','')
    self.jsagaDIR     = gConfig.getValue(self.section+'/JSAGADir','')
    self.broker     = gConfig.getValue(self.section+'/Broker','')

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )

  #############################################################################
  def submitJob(self, executableFile, proxy, numberOfPilots=1):
    """ Method to submit job
    """
    print "******************************************************"
    print "ExecutableFile: ", executableFile
    print "Proxy: ", proxy
    print "localID", localID
    print "******************************************************"

    pilotList = []

    self.log.info("Executable file path: %s" %executableFile)
    if not os.access(executableFile, 5):
      os.chmod(executableFile,0755)

    if self.gridEnv:
      self.log.verbose( 'Sourcing JSAGAEnv script:', self.gridEnv )
      ret = Source( 10, [self.JSAGAEnv] )
      if not ret['OK']:
        self.log.error( 'Failed sourcing JSAGAEnv:', ret['Message'] )
        return S_ERROR( 'Failed sourcing JSAGAEnv' )
      if ret['stdout']: self.log.verbose( ret['stdout'] )
      if ret['stderr']: self.log.warn( ret['stderr'] )
    else:
      self.log.error("JSAGA Environment must be defined in the configuration \
                      file before submit jobs using jsaga")

    cmd = """jsaga-job-run.sh \
-Output StdOut \
-Error  StdError \
-r wms://%s:7443/glite_wms_wmproxy_server \
-Executable %s \
-FileTransfer '%s/dirac-pilot.py\>dirac-pilot.py,
               %s/dirac-install.py\>dirac-install.py,
               %s\>%s,
               StdOut\<StdOut,
               StdError\<StdError' \
"""  % (self.Broker, executableFile, self.jsagaDir, self.jsagaDir, executableFile, executableFile)


    print "COMMAND: ", cmd
    self.log.verbose('JSAGA submission command: %s' %(cmd))

    for i in range(1, numberOfPilots):

      result = shellCall(0, cmd, callbackFunction = self.sendOutput)

      if not result['OK'] or result['Value'][0]:
        self.log.warn('===========>JSAGA result NOT OK')
        self.log.debug(result)
        return S_ERROR(result['Value'])
      else:
        self.log.debug('JSAGA result OK')
        pilotRef = result['Value']
        print "Pilot Reference: ", pilotRef
        pilotList.append(pilotRef)
        self.submittedJobs += 1

    return S_OK(pilotList)

  #############################################################################
  def getJobStatus(self, pilotList):
    """ Method to return information on running and pending jobs.
    """

    pilotsStatus = {}
    numScheduled = 0
    numRunning = 0
    numDone = 0
    numFailed = 0

    if self.gridEnv:
      self.log.verbose( 'Sourcing JSAGAEnv script:', self.gridEnv )
      ret = Source( 10, [self.JSAGAEnv] )
      if not ret['OK']:
        self.log.error( 'Failed sourcing JSAGAEnv:', ret['Message'] )
        return S_ERROR( 'Failed sourcing JSAGAEnv' )
      if ret['stdout']: self.log.verbose( ret['stdout'] )
      if ret['stderr']: self.log.warn( ret['stderr'] )
    else:
      self.log.error("JSAGA Environment must be defined in the configuration \
                      file before submit jobs using jsaga")

    for pilot in pilotList:

      cmd = ["jsaga-job-info.sh", pilot ]
      ret = executeGridCommand( proxy, cmd, jsagaEnv )
      print "RET: ", ret

      if not ret['OK']:
        self.log.error( 'Error r', ret['Message'] )

      else:
        retValue = ret['Value']
        status= retValue.split("\n")[0].split(":")[1].split()[0]
        result1= retValue("\n")[4].split("time:  ")[1]
        dt = datetime.datetime.fromtimestamp(time.mktime((time.strptime(result1,"%a %b %d %H:%M:%S %Z %Y"))))
        ce = retValue.split("\n")[7].split()[1]

        print "Status: ", status
        if status == "RUNNING" and ce=="":
          status = "SCHEDULED"
          numScheduled += 1

        elif status == "RUNNING" and ce!="":
          numRunning += 1

        elif status=="DONE":
          numDone += 1

        elif status=="FAILED":
          numFailed += 1

        else:
          print "Error to retrieve the status %s" % pilot

        pilotsStatus[pilot]=status
        print pilotsStatus

      print "PILOT STATUS: ", pilotsStatus

    return S_OK(pilotsStatus)

  #############################################################################
  def getJobOutput(self, pilotID, proxy, dir=None):
    """ Method to return StdOut and StdError from a specific pilot job.
    """

    if dir=="":
      workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'JSAGA_' )
      print "Working directory: ", workingDirectory
    else:
      workingDirectory = dir
      print "Working directory = Dir", dir

    os.chdir( workingDirectory )

    if self.gridEnv:
      self.log.verbose( 'Sourcing JSAGAEnv script:', self.gridEnv )
      ret = Source( 10, [self.JSAGAEnv] )
      if not ret['OK']:
        self.log.error( 'Failed sourcing JSAGAEnv:', ret['Message'] )
        return S_ERROR( 'Failed sourcing JSAGAEnv' )
      if ret['stdout']: self.log.verbose( ret['stdout'] )
      if ret['stderr']: self.log.warn( ret['stderr'] )
    else:
      self.log.error("JSAGA Environment must be defined in the configuration \
                      file before submit jobs using jsaga")

    cmd = ["jsaga-job-get-output.sh", pilot ]
    ret = executeGridCommand( proxy, cmd, jsagaEnv )

    print "RET: ", ret

    if not ret['OK']:
      self.log.error('Failed to retrieve output files from %s' %(pilotID))
      shutil.rmtree( workingDirectory )
      print "WORKING DIRECTORY REMOVED"
      return S_ERROR('Failed to get output files')
    else:
      pilotOutput={}
      filename = ("stdOut")
      f = open (filename, "r")
      stdOutput = f.read()
      f.close()

      filename = ("stdError")
      f = open (filename, "r")
      stdError = f.read()
      f.close()

      result[StdOut] = stdOutput
      result[StdError] = stdError
      shutil.rmtree( workingDirectory )
      print "Removiendo el directorio"

    return S_OK(result)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
