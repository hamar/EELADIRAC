########################################################################
# $Id:  $
########################################################################
"""
MPIService class. It matches MPI Agent Site capabilities to MPI job requirements
using Matcher Service.
"""

__RCSID__ = ""

import re, os, sys, time, commands
import string
import signal, fcntl, socket
import getopt
from   types import *
import threading

from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB           import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB    import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from EELADIRAC.WorkloadManagementSystem.DB.MPIJobDB    import MPIJobDB
from DIRAC.Core.DISET.RPCClient                        import RPCClient
from DIRAC                                             import gMonitor
from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler
from DIRAC.Core.Utilities.ClassAd.ClassAdLight         import ClassAd
from DIRAC.FrameworkSystem.Client.ProxyManagerClient   import gProxyManager
from DIRAC.Core.Security.Misc                          import getProxyInfo
from DIRAC.Core.Security                               import File


slock = threading.Lock()
gMutex = threading.Semaphore()
gTaskQueues = {}
jobLoggingDB = False
taskQueueDB = False
mpiJobDB = False
jobDB = False
SUMMARY = [ 'RingID', 'JobID','Status','Site', 'CE', 'Platform', 'Master', 
            'Port',  'NumberOfProcessorsJob', 'NumberOfProcessorsRing', 
            'TimeNew', 'LastTimeUpdate', 'Flavor','ExecutionTime' ]

###############################################################################
def initializeMPIServiceHandler(serviceInfo):
  """  MPIService initialization
  """

  global mpiJobDB
  global jobLoggingDB
  global taskQueueDB
  global matcher
  global ringID
  global jobDB
  mpiJobDB = MPIJobDB()
  jobDB = JobDB()
  ringID = False
  status = False
  hostname = False
  numProc = 1
  slaveUP = False
  jobStatus = False
  jobMatch = False
  resourceJDL = False
  directory = False
  platform = False
  gridCE = False
  masterFlag = False
  slaveFlag = False
  return S_OK()

class MPIServiceHandler(RequestHandler):

###############################################################################
  types_matchRing = []
  def export_matchRing(self, matchDict, statusDict):
    """ Serve to MPIAgent if exist a Ring in a particular site 
        Inputs: matchDict = {Site,CE,Platform}
                statusDict = {Hostname, Master, Slave, ResourceJDL, PilotReference}
        Output: result = {JobID, Status, PilotID, RingID, MasterFlag, SlaveFlag,
                          JobMatch, MPIFlavor}
    """ 
    result = self.__match(matchDict, statusDict)
    if not result['OK']:
      gLogger.info(result['Message'])
      return S_ERROR(result['Message'])
    return S_OK(result['Value'])

#################################################################################
  def __match(self, matchDict, statusDict):
    """ This function ask to MPIJobDB if a ring exists in a particular site
        Input:  matchDict {Site,CE,Platform}
                statusDict {Hostname, Master, Slave, ResourceJDL, PilotReference}
        Output: result = {JobID, Status, PilotID, RingID, MasterFlag, SlaveFlag,
                          JobMatch, MPIFlavor}
    """
    
    resourceJDL = statusDict['ResourceJDL']
    masterFlag  = statusDict['Master']
    slaveFlag   = statusDict['Slave']
    site        = matchDict['Site']
    platform    = matchDict['Platform']
    hostname    = statusDict['Hostname']
    gridCE      = matchDict['CE']
    pilotReference = statusDict['PilotReference']

    slock.acquire()
    matchDict = {'Site':site, 'Platform':platform, 'CE':gridCE}
    result = mpiJobDB.selectRing1(matchDict)
    if result['OK']:
      ringID = int(result['Value']['RingID'])
      status = result['Value']['Status']
      jobID  = int(result['Value']['JobID'])
      gLogger.info("-------------------------------------------------------------------")
      gLogger.info(('Match Ring Results: Ring ID: %s JobID: %s  Status: %s ') % (ringID, jobID, status))
      gLogger.info("-------------------------------------------------------------------")
    else:
      slock.release()
      gLogger.info('ERROR to select the ring')
      return S_ERROR(result['Message'])

    if status == 'Empty':
       gLogger.info("-------------------------------------------------------------------")
       gLogger.info("Ring Status = EMPTY")
       gLogger.info("-------------------------------------------------------------------")
       result = self.matchJob(resourceJDL)
       if not result['OK']:
         gLogger.info("-------------------------------------------------------------------")
         gLogger.info("Is not a MPI Job into TaskQueue")
         gLogger.info("-------------------------------------------------------------------")
         slock.release()
         gLogger.error(result['Message'])
         return S_ERROR(result['Message'])
       else:
         masterTest = mpiJobDB.matchMaster(hostname)
         if masterTest['Value']=='NO':
           gLogger.info("Master exist, job is going to be rescheduled")
           slock.release()
           jobID = result['Value']['JobID']
           rescheduleJobs = jobDB.rescheduleJob(jobID)
           return S_ERROR(['Failed, Master Exist'])
         else:
           gLogger.info("-----------------------------------------")
           gLogger.info(('Job Matched: %s') % (result['Value']['JobID']))
           gLogger.info("-----------------------------------------")


       jobMatch = result['Value']
       numProc = int(result['Value']['NumProc'])
       jobID = result['Value']['JobID']
       mpiFlavor = result['Value']['MPIFlavor']
       gLogger.info("-----------------------------------------")
       gLogger.info(('MPI FLAVOR: %s') % (mpiFlavor))
       gLogger.info("-----------------------------------------")
       masterFlag = True
       status = 'Accumulating'
       createDict = {'JobID': jobID, 'Status': status, 'Site': site, 'CE': gridCE, 'Platform': platform, 
                     'Master': hostname, 'Port': 0, 'NumberOfProcessorsJob': numProc, 'Flavor': mpiFlavor}
       minor = "Accumulating slaves"
       cmd = ("lcg-infosites --vo prod.vo.eu-eela.eu ce|grep -m 1 %s|awk '{#print $NF}'") % (gridCE)
       status, resource = commands.getstatusoutput(cmd)
       if status==0:
         gLogger.debug("lcg-infosites command ok")
       else:
         gLogger.debug("lcg-infosites command NOT ok")
         
       self.setMinorStatus(jobID, minor)
       result = self.createRing(createDict)
       if not result['OK']:
         slock.release()
         rescheduleJobs = jobDB.rescheduleJob(jobID)
         gLogger.error("-------------------------------------------------------------------")
         gLogger.error('Failed to create a new ring')
         gLogger.error("-------------------------------------------------------------------")
         gLogger.error(result['Message'])
         return S_ERROR(result['Message'])
         
       ringID = result['Value']['RingID']
       pilotType = 'Master'
       rank = 0
       addDict = {'RingID': ringID, 'JobID': jobID, 'PilotType': pilotType, 'Status':'New', 
                  'Hostname': hostname, 'ResourceJDL': resourceJDL, 
                  'PilotJobReference': pilotReference, 'Rank':rank}
       result = self.addHostToRing(addDict)
       if not result['OK']:
         slock.release()
         gLogger.error("-------------------------------------------------------------------")
         gLogger.error('Failed to add host to the ring')
         gLogger.error("-------------------------------------------------------------------")
         rescheduleJobs = jobDB.rescheduleJob(jobID)
         if not result['OK']:
           gLogger.error("-------------------------------------------------------------------")
           gLogger.error('Failed to reschedule the job')
           gLogger.error("-------------------------------------------------------------------")
           gLogger.error(result['Message'])
         return S_ERROR(result)
       else:
	     pilotID = result['Value']
	     gLogger.info ("-----------------------------------------")
	     gLogger.info (("The pilot ID assigned: %s and type: %s ") % (pilotID, pilotType))
	     gLogger.info ("-----------------------------------------")

    elif status == 'Accumulating':
      gLogger.info ("-----------------------------------------")
      gLogger.info (("Status:%s JobID:%s RingID:%s") % (status, jobID, ringID))
      gLogger.info ("-----------------------------------------")

      if slaveFlag == False and masterFlag == False:
        slaveFlag = True
        pilotType = 'Slave'
        rank = 1
        addDict = {'RingID': ringID, 'JobID': jobID, 'PilotType': pilotType, 'Status':'New', 
                   'Hostname': hostname, 'ResourceJDL': resourceJDL, 
                   'PilotJobReference': pilotReference, 'Rank':rank}
        result = self.addHostToRing(addDict)
        if not result['OK']:
        ## OJO CON LOS SLOCK RELEASE
          slock.release()
	  gLogger.error("-------------------------------------------------------------------")
	  gLogger.error('Failed to add slave host to the ring')
	  gLogger.error("-------------------------------------------------------------------")
	  rescheduleJobs = jobDB.rescheduleJob(jobID)
	  if not result['OK']:
	    gLogger.error("-------------------------------------------------------------------")
	    gLogger.error('Failed to reschedule the job')
	    gLogger.error("-------------------------------------------------------------------")
	    gLogger.error(result['Message'])
	    return S_ERROR(result)
	pilotID = result['Value']
	gLogger.info ("-----------------------------------------")
	gLogger.info (("The pilot ID assigned: %s and type: %s ") % (pilotID, pilotType))
	gLogger.info ("-----------------------------------------")
        jobMatch = False
        slock.release()
      if masterFlag == True:
         name1 = time.time()
         namePilots = ("/tmp/%s") % (name1)
         res = self.__prepareSecurityDetails()
         gLogger.info("Pilot number to be submited: %s") % (numProc)
         #for i in range(numProc):
         #cmd = (('glite-wms-job-submit -a -o %s -r %s /opt/dirac/pro/EELADIRAC/WorkloadManagementSystem/PilotAgent/eela.jdl') % (namePilots, resource))
         #ret = commands.getoutput(cmd)
         #print "==== GLITE ====================================="
         #print ret
         #print "========================================="
         #cmd = ("python /opt/dirac/pro/EELADIRAC/WorkloadManagementSystem/scripts/dirac-admin-submit-pilot-for-job.py %s /opt/dirac/pro/EELADIRAC/WorkloadManagementSystem/PilotAgent/eela.jdl") % (JobID, gridCE)
         #resource = commands.getoutput(cmd)
         
    result1 = self.getMPIFlavor(addDict)
    if not result1['OK']:
      gLogger.info ("-----------------------------------------")
      gLogger.info ("Failed to get MPI Flavor")
      gLogger.info ("-----------------------------------------")
      gLogger.error(result1['Message'])
      return S_ERROR(result1)
   
    mpiFlavor = result1['MPIFlavor']
    result = {'JobID':jobID, 'Status': status, 'PilotID': pilotID, 'RingID':ringID, 
              'MasterFlag': masterFlag, 'SlaveFlag':slaveFlag, 'JobMatch':jobMatch, 
              'MPIFlavor':mpiFlavor}

    return S_OK(result)

################################################################################

  types_getRingStatus = []
  def export_getRingStatus(self, getDict):
    """ Serve to MPIAgent status of pilots accumulated in the ring site before 
        start MPI daemons
        Input: getDict = {RingID, JobID}
        Output: result = {RingID, Status, JobID}
    """
    result = self.__getStatus(getDict)
    if not result['OK']:
      gLogger.debug ("-----------------------------------------")
      gLogger.debug ("Failed to get status")
      gLogger.debug ("-----------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])
    
    return S_OK(result['Value'])

###############################################################################

  def __getStatus(self, getDict):
    """ Ask to MPIJob DB about number of pilots accumulated in a particular site 
        before start MPI daemons
        Input: getDict = {RingID, JobID}
        Output: result = {RingID, Status, JobID}
    """
    result = mpiJobDB.getRingStatus(getDict)
    if not result['OK']:
      gLogger.debug ("-----------------------------------------")
      gLogger.debug ("Failed to get  Ring status")
      gLogger.debug ("-----------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])

    return S_OK(result['Value'])

##############################################################################

  types_getRingAttributes = []
  def export_getRingAttributes(self, attDict):
    """ Serve to MPIAgent about ring attributes in a particular site
        Inputs: attDict = {JobID, RingID}
        Output: result { RingID, JobID, Master, Port}
    """
    result = self.__getAttributes(attDict)
    if not result['OK']:
      gLogger.debug ("-----------------------------------------")
      gLogger.debug ("Failed to get ring attributes")
      gLogger.debug ("-----------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])
    return S_OK(result['Value'])

##############################################################################

  def __getAttributes(self, ringID):
    """ Ask to MPIJob DB ring attributes and 
        Inputs: attDict = {JobID, RingID}
        Output: result { RingID, JobID, Master, Port}
    """
    result = mpiJobDB.getRingAttributes(ringID)
    if not result['OK']:
      gLogger.debug ("-----------------------------------------")
      gLogger.debug ("Failed to get ring attributes")
      gLogger.debug ("-----------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])
    return S_OK(result['Value'])

##############################################################################

  types_setRingStatus = []
  def export_setRingStatus(self, statDict):
    """ Set ring status, return new ring return
        Input: {JobID, RingID, Status}
        Output: {JobID, RingID}
    """

    result = self.setStatus(statDict)
    if not result['OK']:
      gLogger.debug ("-----------------------------------------")
      gLogger.debug ("Failed to get ring attributes")
      gLogger.debug ("-----------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR(result['Message'])
    return S_OK(result)

##############################################################################

  def setStatus(self, statDict):
    """ Use MPIJob DB to update ring status 
        Input: addDict = {RingID, JobID, PilotType, Status, Hostname,
                          ResourceJDL, PilotJobReference, Rank}
        Output: PilotID
    """ 
    result = mpiJobDB.setRingStatus(statDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to set ring status')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to set ring status')
    return  S_OK(result['Value'])

##############################################################################

  types_ringExists = []
  def export_ringExists(self, resourceJDL):
    """ Serve to MPIAgent if a ring exists in a particular site 
    """
    result = self.exists(resourceJDL)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to check if ring exist')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to check if ring exist')

    return S_OK(result)

##############################################################################

  def exists(self, resourceJDL):
    """ Ask MPIJob DB if a ring exists in a particular site
    """
    result = True
    return result

##############################################################################

  types_setHeartBeat = [ DictType ]
  def export_setHeartBeat(self, resourceDict):
    """ To be implemented 
    """
    return S_OK()

##############################################################################

  types_setMPIReportMethods = [ DictType ]
  def export_setMPIReportMethods(self, resourceDict):
    """ To be implemented 
    """
    return S_OK()

##############################################################################

  types_addHostRing = []
  def export_addHostRing(self, addDict):
    """ MPIJob Agent sent information to add a new host in a ring than is 
        accumulating pilots in a particular site
        Input: addDict = {RingID, JobID, PilotType, Status, Hostname,
                          ResourceJDL, PilotJobReference, Rank}
        Output: PilotID
    """ 

    result = self.addHostToRing(addDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to add host to the ring')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to add host to the ring')
    return S_OK(result['Value'])

##############################################################################

  def addHostToRing(self, addDict):
    """ Add into MPIJob DB a new host into a particular ring
        Input: createDict = {JobID, Status, Site, CE, Platform, Master, Port, NumberOfProcessorsJob, Flavor}
        Output: {RingID, Status, JobID}
    """
    result = mpiJobDB.addHostToRing(addDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to add host to the ring')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to add host to the ring')
    return S_OK(result['Value'])

##############################################################################

  ####types_createRing = []
  def createRing(self, createDict):
    """ Insert the new ring into the database
        Input: createDict = {JobID, Status, Site, CE, Platform, Master, Port, NumberOfProcessorsJob, Flavor}
        Output: {RingID, Status, JobID}
    """
    result = self.createRing1(createDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to create the ring')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to create the ring')
    return S_OK(result['Value'])

##############################################################################

  def createRing1(self, createDict):
    """ Send information to MPIJob DB to insert a new ring in a particular 
        site
        Input: createDict = {JobID, Status, Site, CE, Platform, Master, Port, NumberOfProcessorsJob, Flavor}
        Output: {RingID, Status, JobID}
    """ 
    result = mpiJobDB.createRing(createDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to create the ring')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to create the ring')
    return S_OK(result['Value'])

##############################################################################

  types_testRing = []
  def export_testRing(self, testDict):
    """ Compare the Ring Number of Processors and the Job required processors
        Input: {'RingID': self.ringID, 'JobID': self.jobID, 'Status': self.status,
                'MasterFlag': masterFlag}
        Output: result {NumberOfProcessorsJob, NumberOfProcessorsRing, Status}
    """
    result = self.test(testDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to test the ring')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to test the ring')
    return S_OK(result['Value'])

##############################################################################

  def test(self, testDict):
    """ This function make a comparation between Number of Processors in the 
        ring and Number of Processors required by the job, returns new status
        "Ready" if boths numbers are the same or "Failed" if the time  
        is accumulating pilots more than 600 seconds, job will be rescheduled
        in this case and the mpi variables will be in "False".
        Input: {'RingID': self.ringID, 'JobID': self.jobID, 'Status': self.status,
                'MasterFlag': masterFlag}
        Output: result {NumberOfProcessorsJob, NumberOfProcessorsRing, Status}
    """
    ringID = testDict['RingID']
    jobID = testDict['JobID']
    startFlagTime = time.time()
    maxFlagTime = 600
    result = mpiJobDB.testRing(testDict)
    numProcJob = result['Value']['NumberOfProcessorsJob']
    numProcRing = result['Value']['NumberOfProcessorsRing']
    gLogger.info ("-------------------------------------------")
    gLogger.info (("ProcRing: %s ProcJob: %s") % (numProcRing, numProcJob))
    gLogger.info ("-------------------------------------------")
    while numProcRing <  numProcJob:
      result = mpiJobDB.testRing(testDict)
      #print result
      numProcRing = result['Value']['NumberOfProcessorsRing']
      numProcJob = result['Value']['NumberOfProcessorsJob']
      if testDict['Status']=='Accumulating' and testDict['MasterFlag']==True:
        comparationTime = time.time() - startFlagTime
        if comparationTime > maxFlagTime:
          gLogger.info("-------------------------------------------------------------------")
          gLogger.info("Comparation time in test is longer than allowed, Job to be rescheduled")
          gLogger.info("-------------------------------------------------------------------")
          rescheduleJobs = jobDB.rescheduleJob(jobID)
          status = 'Failed'
          statDict = {'RingID': ringID, 'JobID': jobID, 'Status': status}
          result2 = self.setStatus(statDict)
          gLogger.info ("More than 10 minutes accumulating, job rescheduled")
          return S_ERROR('More than 10 minutes accumulating pilots')
    gLogger.info("-------------------------------------------------------------------")
    gLogger.info(" Time OK, status READY")
    gLogger.info("-------------------------------------------------------------------")
    status = 'Ready'
    minor = "Slave Pilot Complete"
    self.setMinorStatus(jobID, minor)
    statDict = {'RingID': ringID, 'JobID': jobID, 'Status': status}
    result = self.setStatus(statDict)
    result = {'NumberOfProcessorsRing':numProcRing, 
              'NumberOfProcessorsJob':numProcJob, 'Status':status}
    #print result
    return S_OK(result)

##############################################################################

  types_updateProcessors = []
  def export_updateProcessors(self, updDict):
    """ Funtion than update number of processors into a ring
    """

    result = self.updateProc(updDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to update number processors')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to update number processors')
    return S_OK(result['Value'])

##############################################################################

  def updateProc(self, updDict):
    """ Use MPIJob DB to update number of processors into a ring
    """ 
    result = mpiJobDB.updateProcessors(updDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to update Processors')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to update number of processors')
    return S_OK(result['Value'])

##############################################################################

  types_updateRing = []
  def export_updateRing(self, updDict):
    """ Insert the new ring into the database
        Input: {RingID, Port, JobID}
        Output:
    """
    result = self.updRing(updDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to update ring status')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to update ring status')
    return S_OK(result['Value'])

##############################################################################

  def updRing(self, updDict):
    """ Update ring 
        Input: {RingID, Port, JobID}
        Output:
    """ 
    result = mpiJobDB.updateRing(updDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to update ring status')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to update ring status')
    return S_OK(result['Value'])

##############################################################################

  types_getJobOptParameters = []
  def export_getJobOptParameters (self, jobID):
    """ Use Job DB to get job parameters, returns Owner, Group and JDL using 
        jobID as input.  
    """ 
    resultDict = {}
    resOpt = jobDB.getJobOptParameters(jobID)
    if resOpt['OK']:
      for key, value in resOpt['Value'].items():
        resultDict[key] = value
    resAtt = jobDB.getJobAttributes(jobID, ['OwnerDN', 'OwnerGroup'])
    if not resAtt['OK']:
      return S_ERROR('Could not retrieve job attributes')
    if not resAtt['Value']:
      return S_ERROR('No attributes returned for job')

    resultDict['DN'] = resAtt['Value']['OwnerDN']
    resultDict['Group'] = resAtt['Value']['OwnerGroup']
    result = jobDB.getJobJDL(jobID)
    resultDict['JDL'] = result['Value']

    matcherParams = ['JDL', 'DN', 'Group']
    for p in matcherParams:
      if not resultDict.has_key(p):
        gLogger.error (jobID, 'Failed', 'Matcher did not return %s' %(p))
      elif not resultDict[p]:
        gLogger.error(jobID, 'Failed', 'Matcher returned null %s' %(p))
      else:
        gLogger.info('Matcher returned %s = %s ' %(p, resultDict[p]))
    return S_OK(resultDict)

#############################################################################

  types_monitorRing = []
  def export_monitorRing (self, monDict):
    """ This function use JobMonitoring service to know the job status  
        while the job is running
        Input: {JobID, RingID, PilotID}
        Output: Status
    """ 
    jobID = int(monDict['JobID'])
    ringID = int(monDict['RingID'])
    pilotID = int(monDict['PilotID'])
    jobMonitor=RPCClient('WorkloadManagement/JobMonitoring')
    result = jobMonitor.getJobStatus(jobID)
    gLogger.info (("Job Status: %s, Job ID:%s") % (result['Value'], jobID))
    if not result['OK']:
      gLogger.error('Failed to get the job status')
      gLogger.error(result['Message'])
      return S_ERROR('Failed to get the job status')
    status1 = result['Value']
    status = status1
    while status1 == status and status<>'Done' and status<>'Failed':
      result = jobMonitor.getJobStatus(jobID)
      if not result['OK']:
        gLogger.error('Failed to get the job status')
        gLogger.error(("Job Status Error: %s") % (result['Message']))
        return S_ERROR('Failed to get the job status')
      status = result['Value']
      gLogger.info (("Job ID:%s - Status:%s") % (jobID, status))
    statDict = {'RingID': ringID, 'JobID': jobID, 'Status': status}
    status = self.setStatus(statDict)
    result = status['Value']['Status']
    return S_OK(result)

################################################################################

  def matchJob(self, resourceJDL):
    """  Use Matcher service to retrieve a MPI job from Task Queue.
         Returns: JobID, NumProc required, JDL and MPI flavor
         Input: resourceJDL
         Output: result = {JobID, JobJDL, NumProc, MPIFlavor}
    """
    matcher = RPCClient('WorkloadManagement/Matcher', timeout = 600)
    dictMatchMPI = {'Setup':'EELA-Production', 'CPUTime':6000, 'JobType':'MPI'}
    result = matcher.getMatchingTaskQueues(dictMatchMPI)
    if not result['OK']:
      #print "S38"
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error ("Here I have to call to get normal job")
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error (("Match not found: %s") % (result['Message']))
      gLogger.info("-------------------------------------------------------------------")
      return S_ERROR()
    else:
      if result['Value'] == {}:
        gLogger.info("-------------------------------------------------------------------")
        gLogger.info("Value == Empty")
        gLogger.info("-------------------------------------------------------------------")
        return S_ERROR()
    mpiTaskQueue = result['Value']
    classAdAgent = ClassAd(resourceJDL)
    classAdAgent.insertAttributeString('JobType', 'MPI')
    resourceJDL = str(classAdAgent.asJDL())
    result = matcher.requestJob(resourceJDL)
    if not result['OK']:
      gLogger.error (("Request Job Error: %s") % (result['Message']))
      return S_ERROR()
    elif result['OK']==False:
      gLogger.error (("Request Job False: %s") % (result['Message']))
      return S_ERROR()
    else:
      gLogger.error (("Request Job OK"))
    jobJDL = result['Value']['JDL']
    jobID1 = ClassAd(jobJDL)
    jobID = jobID1.getAttributeString('JobID')
    numProc = jobID1.getAttributeString('CPUNumber')
    mpiFlavor = jobID1.getAttributeString('Flavor')
    result = {'JobID':jobID, 'JobJDL':jobJDL, 'NumProc': numProc, 
              'MPIFlavor': mpiFlavor}
    return S_OK(result)

#############################################################################

  types_startRing = []
  def export_startRing(self, startDict):
     """ Set job status to Starting 
         Input: {JobID, RingID}
         Output: Status
     """ 
     jobID = startDict['JobID']
     ringID = startDict['RingID']
     status = 'Starting'
     statDict = {'RingID': ringID, 'JobID': jobID, 'Status': status}
     status = self.setStatus(statDict)
     if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to set ring status')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to set ring status')
     result = status['Value']['Status']
     return S_OK(result)

#############################################################################

  def setMinorStatus (self, jobID, minor):
       """ Set minor status of the job depending Ring Status 
           Input: jobID, minor
           Output: -
       """
       jobAttrib1 = jobDB.getJobAttribute(jobID, 'Status')
       jobAttrib = jobAttrib1['Value']
       jobMinorStatus = jobDB.setJobStatus(jobID, jobAttrib , minor)
       return

#############################################################################

  def __prepareSecurityDetails(self):
    """ This function get the proxy details to submit the job
    """
    self.defaultProxyLength = gConfig.getValue('/Security/DefaultProxyLifeTime', 86400*5)
    ownerDN = self._clientTransport.peerCredentials['DN']

    clientUsername = self._clientTransport.peerCredentials['username']
    ownerGroup = self._clientTransport.peerCredentials['group']
    retVal = gProxyManager.downloadVOMSProxy(ownerDN, ownerGroup, limited = False, requiredTimeLeft = self.defaultProxyLength)
    if not retVal[ 'OK' ]:
      os.system('dirac-proxy-info')
      sys.stdout.flush()

    chain = retVal[ 'Value' ]
    proxyChain = chain
    proxy = proxyChain.dumpAllToString()
    payloadProxy=proxy['Value']
    result = File.writeToProxyFile(payloadProxy)
    if not result['OK']:
      return result
    proxyLocation = result['Value']
    return S_OK(chain)

##############################################################################

  types_getMachineFile = []
  """ REVISAR 
  """
  def export_getMachineFile(self, getDict):
    """ Insert the new ring into the database
        Input: testDict = {JobID, RingID}
        Output: hostname list
    """
    result = self.machineFile(getDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to get machine file')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to get machine file')
    return S_OK(result)

##############################################################################

  def machineFile(self, getDict):
    """ Create machine file to run MPICH 1 jobs
        Input: {JobID, RingID}
        Output: Machine File
    """
    result = mpiJobDB.machineFile(getDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to get machine file')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to get machine file')
    return S_OK(result['Value'])

##############################################################################
  types_getMPIFlavor = []
  def getMPIFlavor(self, getDict):
    """ Return MPI Flavor for a ring
        Input: addDict {RingID, JobID, PilotType, Status, Hostname, ResourceJDL, PilotJobReference, Rank}
        Output: {MPIFlavor, JobID}
    """
    result = self.getFlavor(getDict)
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to get machine file')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to get machine file')
    return (result['Value'])

##############################################################################
  def getFlavor(self, getDict):
    """ Input: addDict {RingID, JobID, PilotType, Status, Hostname, ResourceJDL, PilotJobReference, Rank}
        Output: {MPIFlavor, JobID}
    """
 
    result = mpiJobDB.getMPIFlavor(getDict) 
    if not result['OK']:
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error('Failed to add host to the ring')
      gLogger.info("-------------------------------------------------------------------")
      gLogger.error(result['Message'])
      return S_ERROR('Failed to set Ring Status')  
    return (result['Value'])
    
#############################################################################   
   
    
    
##############################################################################
##############################################################################
##############################################################################
  types_getRingsPageSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getRingsPageSummaryWeb(self, selectDict, sortList, startItem, maxItems, selectRings = True):
    """ Get the summary of the ring information for a given page in the
        ring monitor in a generic format
    """

    
    resultDict = {}
    startDate = selectDict.get('FromDate',None)
    if startDate:
      del selectDict['FromDate']
    # For backward compatibility
    if startDate is None:
      startDate = selectDict.get('LastUpdate',None)
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get('ToDate',None)
    if endDate:
      del selectDict['ToDate']  
    jobID = selectDict.get('JobID',None)
    #print "JOB ID", jobID
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None
    #print "SELECT DICTIONARY", selectDict, "***************************************************-++++++"
    if selectRings:
      result = mpiJobDB.selectRingsWeb(selectDict, orderAttribute=orderAttribute,
                                newer=startDate, older=endDate )
      #print "RESULT SELECT RINGS WEB --------------------------->", result
      if not result['OK']:
        return S_ERROR('Failed to select Rings: '+result['Message'])
 
      ringList = result['Value']
      #print "RINGGGGGGGGGGGG LISTTTTTTTTTTTTTTTTT", ringList
      nRings = len(ringList)
      resultDict['TotalRecords'] = nRings
      if nRings == 0:
        return S_OK(resultDict)
 
      iniRing = startItem
      lastRing = iniRing + maxItems
      if iniRing >= nRings:
        return S_ERROR('Item number out of range')
 
      if lastRing > nRings:
        lastRing = nRings
 
      summaryRingList = ringList[iniRing:lastRing]
      #print ">>>>>>>>>>summaryRingList ", summaryRingList 
      result = mpiJobDB.getAttributesForRingList(summaryRingList,SUMMARY)
      #####result = mpiJobDB.getAttributesForRingList(ringList,SUMMARY)
      if not result['OK']:
        return S_ERROR('Failed to get Ring summary: '+result['Message'])
      #print "RESULT GET RING ATTRIBUTES FOR RING LIST", result 
      summaryDict = result['Value']
      #print "summaryDict ... ", summaryDict 
      # Evaluate last sign of life time
      ####for ringID, ringDict in summaryDict.items():
      ####  if ringDict['HeartBeatTime'] == 'None':
      ####    ringDict['LastSignOfLife'] = ringDict['LastUpdateTime']
      ####  else:
      ####    lastTime = Time.fromString(ringDict['LastUpdateTime'])
      ####    hbTime = Time.fromString(ringDict['HeartBeatTime'])
      ####    if (hbTime-lastTime) > (lastTime-lastTime) or jobDict['Status'] == "Stalled":
      ####      ringDict['LastSignOfLife'] = ringDict['HeartBeatTime']
      ####    else:
      ####      ringDict['LastSignOfLife'] = ringDict['LastUpdateTime']
     
      # prepare the standard structure now
      key = summaryDict.keys()[0]
      paramNames = summaryDict[key].keys()
      #print key,paramNames,"key, params <<<<<<<<<<<<<<<<<<"
      records = []
      for ringID, ringDict in summaryDict.items():
        rParList = []
        #print rParList, "FRO RING ID LIST"
        for pname in paramNames:
          rParList.append(ringDict[pname])
        records.append(rParList)
 
      resultDict['ParameterNames'] = paramNames   
      resultDict['Records'] = records
   
    statusDict = {}
    result = mpiJobDB.getCounters('Rings',['Status'],selectDict,
                               newer=startDate,
                               older=endDate,
                               timeStamp='LastUpdateTime')
    if result['OK']:
      for stDict,count in result['Value']:
         statusDict[stDict['Status']] = count
    resultDict['Extras'] = statusDict

    #print resultDict, "<<<<<<<<<<< RESULTADO DICT"
    return S_OK(resultDict)
##############################################################################
  types_getSiteSummary = [ ]
  def export_getSiteSummary( self ):
    return mpiJobDB.getSiteSummary()
##############################################################################
  types_killRing = [ ]
  def export_killRing (self, ringIDs):
    """  Kill jobs specified in the ringIDs list
    """

    ringList = self.__get_ring_list(ringIDs)
    if not ringList:
      return S_ERROR('Invalid ring specification: '+str(ringIDs))

    for ringID in ringList:
      result = mpiJobDB.killRing(ringID,'Kill')
      if not result['OK']:
        bad_ids.append(ringID)
      else:
        gLogger.info('Ring %d is marked for termination' % ringID)
        good_ids.append(ringID)

    result = S_OK( ringList )
    return result
###########################################################################
  def __get_ring_list( self, ringInput ):
    """ Evaluate the ringInput into a list of ints
    """

    if type(ringInput) == IntType:
      return [ringInput]
    if type(ringInput) == StringType:
      try:
        iring = int(ringInput)
        return [ijob]
      except:
        return []
    if type(ringInput) == ListType:
      try:
        lring = [ int(x) for x in ringInput ]
        return lring
      except:
        return []

    return []

##############################################################################
  types_getSites = []
  def export_getSites (self):
    """ Return Distict Values of Site Ring Attribute in WMS
    """
    return mpiJobDB.getDistinctRingAttributes('Site')

##############################################################################
  types_getStatusMPI = []
  def export_getStatusMPI (self):
    """ Return Distict Values of Status job Attribute in WMS
    """
    return mpiJobDB.getDistinctRingAttributes('Status')
##############################################################################
  types_getHostnames = []
  def export_getHostnames (self,ringID,jobID):
    """ Return hostnames into a ring
    """
    getDict={'RingID':ringID,'JobID':jobID}
    result=mpiJobDB.machineFile(getDict)
    
    return result

##############################################################################
  types_getOwners = []
  def export_getOwners (self):
    """
    Return Distict Values of Owner job Attribute in WMS
    """
    return S_OK()

