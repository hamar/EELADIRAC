""" 
   ========
   MPIAgent
   ========
  
   The Job MPI Agent is an interface between a capable CE to run MPI Jobs and DIRAC WMS.
   
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.ModuleFactory                       import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                import ClassAd
from DIRAC.Core.Utilities.TimeLeft.TimeLeft                   import TimeLeft
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.Resources.Computing.ComputingElementFactory        import ComputingElementFactory
from DIRAC.Resources.Computing.ComputingElement               import ComputingElement
from DIRAC                                                    import S_OK, S_ERROR, gConfig, platform
from DIRAC.FrameworkSystem.Client.ProxyManagerClient          import gProxyManager
from DIRAC.Core.Security.Misc                                 import getProxyInfo
from DIRAC.Core.Security                                      import Locations
from DIRAC.Core.Security                                      import Properties
from DIRAC.WorkloadManagementSystem.Client.JobReport          import JobReport
from DIRAC.WorkloadManagementSystem.Client.JobDescription     import JobDescription
from DIRAC.WorkloadManagementSystem.Agent                     import JobAgent
from DIRAC.Core.Base.AgentModule                              import AgentModule

import os, sys, re, string, time, commands, random, popen2

AGENT_NAME = 'WorkloadManagement/MPIAgent'

class MPIAgent(AgentModule):

  #############################################################################

  def initialize(self, loops=0):
    """ Sets default parameters, creates CE instance and initialize MPI variables to "False".
    """
    print "1"
    #Disable monitoring
    self.am_setOption( 'MonitoringEnabled', False )
    #self.log.setLevel('debug') #temporary for debugging
    self.am_setOption( 'MaxCycles', loops )

    ceUniqueID = self.am_getOption( 'CEUniqueID', 'InProcess' )
    localCE = gConfig.getValue( '/LocalSite/LocalCE', '' )
    if localCE:
      self.log.info( 'Defining CE from local configuration = %s' % localCE )
      ceUniqueID = localCE

    ceFactory = ComputingElementFactory( ceUniqueID )
    self.ceName = ceUniqueID
    ceInstance = ceFactory.getCE()
    if not ceInstance['OK']:
      self.log.warn( ceInstance['Message'] )
      return ceInstance

    self.computingElement = ceInstance['Value']
    self.diracRoot = os.path.dirname( os.path.dirname( os.path.dirname( os.path.dirname( __file__ ) ) ) )
    #Localsite options
    self.siteRoot = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
    self.siteName = gConfig.getValue( '/LocalSite/Site', 'Unknown' )
    self.pilotReference = gConfig.getValue( '/LocalSite/PilotReference', 'Unknown' )
    self.defaultProxyLength = gConfig.getValue( '/Security/DefaultProxyLifeTime', 86400 * 5 )
    print "2"
    #Agent options
    # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
    self.cpuFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )
    self.jobWrapperTemplate = os.path.join( self.diracRoot,
                                            self.am_getOption( 'JobWrapperTemplate',
                                                               'DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py' ) )
    self.jobSubmissionDelay = self.am_getOption( 'SubmissionDelay', 10 )
    self.defaultLogLevel = self.am_getOption( 'DefaultLogLevel', 'info' )
    self.fillingMode = self.am_getOption( 'FillingModeFlag', False )
    self.jobCount = 0
    #Timeleft
    self.timeLeftUtil = TimeLeft()
    self.timeLeft = gConfig.getValue( '/Resources/Computing/CEDefaults/MaxCPUTime', 0.0 )
    self.gridCEQueue = gConfig.getValue( '/Resources/Computing/CEDefaults/GridCEQueue', '' )
    self.timeLeftError = ''
    self.scaledCPUTime = 0.0
    #### Different for normal jobs
    self.slaveFlag = False
    self.masterFlag = False
    self.master = False
    self.port = False
    self.ringID = False
    self.status = False
    self.hostname = False
    self.numProc = 1
    self.slaveUP = False
    self.jobStatus = False
    self.jobMatch = False
    self.resourceJDL = False
    self.directory = False
    self.platform = False
    self.gridCE = False
    self.jobPath = False
    self.mpiFlavor = False 
    self.sharedHome = False
    return S_OK()

  ##############################################################################################

  def execute(self):
    """ MPI Job Agent Execution Method.
        
        Ring Status:
        ------------
        Exit : When job execution is finished.  
 
    """
    while self.status <> 'Exit':
     if  self.masterFlag == False and self.slaveFlag == False:
       print "3" 
       result = self.computingElement.getJDL()
       if result['OK']:
         print "4" 
         self.resourceJDL = result['Value']
         classAdAgent = ClassAd(self.resourceJDL)
         self.site = classAdAgent.get_expression('Site')
         self.gridCE = classAdAgent.get_expression('GridCE')
         self.platform = classAdAgent.get_expression('LHCbPlatform')
          
         cmd = ('lcg-infosites --vo prod.vo.eu-eela.eu ce |grep %s |cut -d" " -f2') % (self.gridCE)
         self.numProcSite = commands.getoutput(cmd)
         self.hostname = commands.getoutput('/bin/hostname -f')
         self.sharedHome = commands.getoutput ('echo $MPI_SHARED_HOME')
         if self.sharedHome=="":
           print "5"
           self.sharedHome=commands.getoutput('echo $HOME')
         print "SHARED HOME------------->>>>>>>>>>>>", self.sharedHome 
         self.directory = classAdAgent.get_expression('WorkingDirectory')
         print "WORKING DIRECTORY ------------>>>>>>>>>>>>", self.directory
         print "5.1" 
         matchDict = {'Site': self.site, 'CE': self.gridCE, 'Platform': self.platform}
         statusDict = {'Hostname':self.hostname, 'Master': self.masterFlag, 'Slave': self.slaveFlag, 'ResourceJDL': self.resourceJDL, 'PilotReference':self.pilotReference}
         print "5.2" 
         self.mpiService = RPCClient('WorkloadManagement/MPIService')
         print "5.3"
         result = self.mpiService.matchRing(matchDict, statusDict)
         print "====================================================="
         print "====================================================="
         print result
         print "====================================================="
         print "====================================================="

         if result['OK']:
             self.ringID = result['Value']['RingID']
             self.status = result['Value']['Status']
             self.jobID = result['Value']['JobID']
             self.pilotID = result['Value']['PilotID']
             self.masterFlag = result['Value']['MasterFlag']
             self.slaveFlag = result['Value']['SlaveFlag']
             self.jobMatch = result['Value']['JobMatch']
             self.mpiFlavor = result['Value']['MPIFlavor']
             if self.mpiFlavor == "MPICH2":
               print "7"
               res = self.MPICH2environment()
               if not res['OK']:
                 print "MPICH1 environment failed to be initialized"
             elif self.mpiFlavor == 'MPICH1':
               print "8" 
               res = self.MPICH1environment()
               if not res['OK']:
                 print "MPICH1 environment failed to be initialized"
             else:
               print "9"
               Message = 'Problems to start MPICH environment'
               res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
               if not res['OK']:
                 print "Problems to reschedule and finalize" 
               print res
               return S_ERROR('Message')

             self.jobPath = (self.sharedHome+os.sep+str(self.jobID))
             print "SELF JOB PATH", self.jobPath 
             self.log.info("----------------------------------------------------------------------------")
             self.log.info(('Match Ring Results: Ring ID: %s JobID: %s  Status: %s MASTER FLAG: %s  MPI Flavor: %s') % (self.ringID, self.jobID, self.status, self.masterFlag, self.mpiFlavor))
             self.log.info("----------------------------------------------------------------------------")

         else:
             print result
             print "10"
             ####self.log.warn(result['Value']['Message'])
             ####self.log.warn(result['Message'])
             self.log.warn('Failed to match ring')
             #########self.__finish('Not match found')
             self.finalize()
             return S_ERROR('Message')

         ####else:
           ####print "11"
           ####self.log.warn(result['Message'])
           #####self.log.warn('Failed to get the Resource JDL')
           ####return S_ERROR(result)

     elif self.status == 'Accumulating':
       print "12"
       self.log.info ("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info ("-----------------------------------------")
       os.environ['TMP_DIR'] = (str(self.jobID))
       cmd = ('echo $TMP_DIR')
       x = commands.getoutput(cmd)        
       print x 
      

       if self.masterFlag == True:
         print "13"
         testDict = {'RingID': self.ringID, 'JobID':self.jobID, 'Status':self.status, 'MasterFlag':self.masterFlag}
         result = self.mpiService.testRing(testDict)
         if not result['OK']:
           ########self.status = 'Failed'
           print "14" 
           self.log.warn('Failed to get the number of Processors')
           ########self.log.warn(result['Message'])
           Message = 'Failed to get the number of processors'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             print "Problems to reschedule and finalize"
             print res
           return S_ERROR('Failed to get the number of Processors')
         elif result['Value'].has_key('OK'):
           #######self.status = 'Failed'
           print "15"
           Message = 'More than 10 minutes accumulating pilots'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process') 
           return S_ERROR('More than 10 minutes accumulating pilots')
         self.numProcJob = result['Value']['NumberOfProcessorsJob']
         self.numProcRing = result['Value']['NumberOfProcessorsRing']
         self.status = result['Value']['Status']
         #### Review
         #if self.numProcJob > self.numProcSite:
         # print ("SELF JOB ID", self.jobID)
         # print " numeros proc Job mayores al del site aqui 1u"
         # return self.__rescheduleFailedJob( self.jobID , 'Number of nodes in the site are less than those required by the job' )

       else:
         print "16" 
         result = self.wait()
         print "-------------------- WAIT RESULT ----------------------"
         print result
         print "-------------------------------------------------------" 
         if not result['OK']:
           self.log.error('Failed to get the status')
           # FALTA VER QUE PASA            
         self.status = result['Value']

     elif self.status == 'Ready':
       print "17"
       self.log.info("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info("-----------------------------------------")
       if self.masterFlag == True:
         result = self.masterStart()
         print "------------------------"
         print "result master start", result
         print "------------------------"
         if not result['OK']:
           print "18" 
           self.log.warn('Problems to start the master daemon')
           self.log.warn(result['Message'])
           Message = 'Problems to start master daemon'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')

           return S_ERROR(result['Message'])
         self.port = result['Value']['Port']
         self.master = result['Value']['Master']
         self.log.info("-----------------------------------------")
         self.log.info(("Port: %s Master: %s, JobID: %s") % (self.port, self.master, self.jobID))
         self.log.info("-----------------------------------------")
         
         updDict = {'RingID': self.ringID, 'Port': self.port, 'JobID': self.jobID}
         result = self.mpiService.updateRing(updDict)
         if not result['OK']:
           print "19" 
           self.log.warn('Failed to update the ring port number')
           self.log.warn(result['Message'])
           Message = 'Failed to update the ring port number'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')

           return S_ERROR(result['Message'])
         self.status = result['Value']['Status']
         print "20"

       if self.slaveFlag == True:
         print "21" 
         result = self.wait()
         if not result['OK']:
           self.log.error('Failed to get the status')
           # FALTA VER QUE PASA
         self.status = result['Value']

     elif self.status == 'RingInit':
       print "22" 
       self.log.info("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info("-----------------------------------------")
       if self.slaveFlag == True and self.slaveUP == False:
         print "23" 
         attDict = {'JobID': self.jobID, 'RingID': self.ringID}
         result = self.mpiService.getRingAttributes(attDict)
         if not result['OK']:
           print "24"    
           self.log.warn("Failed to get the master and the port to start mpd slave")
           self.log.warn(result['Message'])
           Message = 'Failed to get the master and the port to start mpd slave'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')
           return S_ERROR('Failed to get the master and the port to start the slave mpd')
         self.master = result['Value']['Master']
         self.port = result['Value']['Port']
         slaveDict = {'Master': self.master, 'Port': self.port}
         result = self.slaveStart(slaveDict)
         if not result['OK']:
           print "25"
           self.log.warn("Failed to start slave daemon")
           self.log.warn(result['Message'])
           Message = 'Failed to start slave daemon'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')
           return S_ERROR('Failed to start slave daemon')
         result = self.wait()
         if not result['OK']:
           self.log.error('Failed to get the status')
           # FALTA VER QUE PASA
         self.status = result['Value']

         ##### CAMBIOS
         print "=================== INPUT SANDBOX SLAVE ====================================" 
         print "JOBMATCH", self.jobMatch
         print "resourceJDL", self.resourceJDL
         print "============================================================================"
         result = self.__downloadInputSandbox( self.jobID)
         print result
         print "============================================================================"
        
         if not result['OK']:
           msg = 'Problems to download the input sandbox in the slave'
           self.log.error(msg)
           return S_ERROR(msg) 
         print "============================================================================" 
         print "26"
         return
       elif self.masterFlag == True:
         print "27"
         result = self.testStatusRing(self.directory)
         #self.status = result['Value']['Status']
         self.status = 'Starting' 
       else:
         print "28"    
         result = self.wait()
         if not result['OK']:
           self.log.error('Failed to get the status')
           # FALTA VER QUE PASA
         self.status = result['Value']
         if self.status == 'Failed':
           print "29"
           ########return self.__rescheduleFailedJob(jobID, 'RingInit more than 50 sec')
           #########updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
           #########result = self.mpiService.setRingStatus(updDict)
           Message = 'RingInit more than 50 sec'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')

           return
       return

     elif self.status == 'Starting':
       print "30" 
       self.log.info("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info("-----------------------------------------")

       if self.masterFlag == True:
         print "31" 

         result = self.submitJobMPI(self.jobMatch, self.resourceJDL)
         if result == 'Failed':
           print "32"
           self.log.warn('Failed to Submit MPI Job')
           self.status = result
           Message = 'Failed to Submit MPI Job'
           res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
           if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')



       elif self.slaveFlag == True:
         print "33"
         result = self.wait()
         if not result['OK']:
           self.log.error('Failed to get the status')
           # FALTA VER QUE PASA

         self.status = result['Value']

     elif self.status == 'Running':
       print "34"
       self.log.info("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info("-----------------------------------------")

       if self.masterFlag == True:
        print "35" 
        monDict = {'RingID': self.ringID, 'Status': self.status, 'JobID':self.jobID, 'PilotID': self.pilotID}
        result = self.mpiService.monitorRing(monDict)
        if result['OK']:
          self.status = result['Value']
          print "Job Status OK" 
        else:
          self.status = 'Failed' 
          print "Failed to get job status"

       elif self.slaveFlag == True:
        print "36" 
        result = self.wait()
        if not result['OK']:
          self.log.error('Failed to get the status')
          # FALTA VER QUE PASA

        self.status = result['Value']

       else:
        print "37"
        return

     elif self.status == 'Done' or self.status == 'Failed':
       print "38" 
       self.log.info("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info("-----------------------------------------")

       if self.masterFlag == True:
         print "39" 
         result = self.shutdownRing()
         self.status = 'OUT'
       else:
         print "40"
         result = self.shutdownMPD()
         self.status = 'OUT'

     elif self.status == 'OUT':
       print "41" 
       self.log.info("-----------------------------------------")
       self.log.info(('Ring ID: %s JobID: %s  Status: %s ') % (self.ringID, self.jobID, self.status))
       self.log.info("-----------------------------------------")
       result = self.__getCPUTimeLeft()
       self.log.info('Result from TimeLeft utility:', result)
       print result
       print "TIME UTILITY RESULT"

       self.finalize()

     else:
       print "42" 
       self.log.info("-----------------------------------------")
       self.log.info("Status is not defined properly into DB, changing status to EXIT")
       self.log.info("-----------------------------------------")
       self.status == 'OUT'
       self.finalize()
       return S_ERROR(self.status)

    return S_OK('JobMPI Agent cycle complete')

  #############################################################################

  def submitJobMPI(self, jobMatch, resourceJDL):
    """  Submit MPI Job to the Grid, its the same than for normal jobs, just variations in order to update MPIJobDB.
    """
    print "43"
    matcherInfo = jobMatch
    jobAtt = self.jobMatch['JobJDL']
    jobJDL = ClassAd(jobAtt)
    jobID = jobJDL.getAttributeInt('JobID')
    jobMatchJDL = ClassAd(jobAtt)
    matcherParams = ['JDL', 'DN', 'Group']
    result = self.mpiService.getJobOptParameters(self.jobID)
    matcherInfo = result['Value']

    self.log.verbose('Job Agent execution loop')
    available = self.computingElement.available()
    if not available['OK']:
      self.log.info('Resource is not available')
      self.log.info(available['Message'])
      return self.__finish('CE Not Available')

    self.log.info(available['Value'])

    ceJDL = self.computingElement.getJDL()
    resourceJDL = ceJDL['Value']
    self.log.verbose(resourceJDL)
    start = time.time()
    matchTime = time.time() - start
    self.log.info('MatcherTime = %.2f (s)' %(matchTime))

    matcherParams = ['JDL', 'DN', 'Group']
    for p in matcherParams:
      if not matcherInfo.has_key(p):
        self.__report(jobID, 'Failed', 'Matcher did not return %s' %(p))
        return self.__finish('Matcher Failed1')
      elif not matcherInfo[p]:
        self.__report(jobID, 'Failed', 'Matcher returned null %s' %(p))
        return self.__finish('Matcher Failed2')
      else:
        self.log.verbose('Matcher returned %s = %s ' %(p, matcherInfo[p]))

    jobJDL = matcherInfo['JDL']
    jobGroup = matcherInfo['Group']
    ownerDN = matcherInfo['DN']

    optimizerParams = {}
    for key in matcherInfo.keys():
      if not key in matcherParams:
        value = matcherInfo[key]
        optimizerParams[key] = value

    parameters = self.__getJDLParameters(jobJDL)
    if not parameters['OK']:
      self.__report(jobID, 'Failed', 'Could Not Extract JDL Parameters')
      self.log.warn(parameters['Message'])
      return self.__finish('JDL Problem')

    params = parameters['Value']

    if not params.has_key('JobID'):
      msg = 'Job has not JobID defined in JDL parameters'
      self.log.warn(msg)
      return S_OK(msg)
    else:
      jobID = params['JobID']

    if not params.has_key('JobType'):
      self.log.warn('Job has no JobType defined in JDL parameters')
      jobType = 'Unknown'
    else:
      jobType = params['JobType']

    if not params.has_key('SystemConfig'):
      self.log.warn('Job has no system configuration defined in JDL parameters')
      systemConfig = 'ANY'
    else:
      systemConfig = params['SystemConfig']

    if not params.has_key('MaxCPUTime'):
      self.log.warn('Job has no CPU requirement defined in JDL parameters')
      jobCPUReqt = 0
    else:
      jobCPUReqt = params['MaxCPUTime']

    self.log.info('Received JobID=%s, JobType=%s, SystemConfig=%s' %(jobID, jobType, systemConfig))
    self.log.info('OwnerDN: %s JobGroup: %s' %(ownerDN, jobGroup))

    try:
      self.__setJobParam(jobID, 'MatcherServiceTime', str(matchTime))
      self.__report(jobID, 'Matched', 'Job Received by Agent')
      self.__setJobSite(jobID, self.siteName)
      self.__reportPilotInfo(jobID)
      ret = getProxyInfo(disableVOMS = True)
      if not ret['OK']:
        self.log.error('Invalid Proxy', ret['Message'])
        self.status = 'Failed'
        updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
        result = self.mpiService.setRingStatus(updDict)

        return self.__rescheduleFailedJob(jobID , 'Invalid Proxy')
    
        

      proxyChain = ret['Value']['chain']
      if not 'groupProperties' in ret['Value']:
        self.log.error('Invalid Proxy', 'Group has no properties defined')
        return self.__rescheduleFailedJob(jobID , 'Proxy has no group properties defined')

      if Properties.GENERIC_PILOT in ret['Value']['groupProperties']:
        proxyResult = self.__setupProxy(jobID, ownerDN, jobGroup, self.siteRoot)
        if not proxyResult['OK']:
          self.log.error('Invalid Proxy', proxyResult['Message'])
          self.status = 'Failed'
          updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
          result = self.mpiService.setRingStatus(updDict)
          return self.__rescheduleFailedJob(jobID , 'Fail to setup proxy')
        else:
          proxyChain = proxyResult['Value']

      saveJDL = self.__saveJobJDLRequest(jobID, jobJDL)

      resourceParameters = self.__getJDLParameters(resourceJDL)
      if not resourceParameters['OK']:
        return resourceParameters
      resourceParams = resourceParameters['Value']

      software = self.__checkInstallSoftware(jobID, params, resourceParams)
      if not software['OK']:
        self.log.error('Failed to install software for job %s' %(jobID))
        errorMsg = software['Message']
        if not errorMsg:
          errorMsg = 'Failed software installation'
          self.status = 'Failed'
          updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
          result = self.mpiService.setRingStatus(updDict)
        return self.__rescheduleFailedJob(jobID, errorMsg)

      self.log.verbose('Before %sCE submitJob()' %(self.ceName))
      submission = self.__submitJob(jobID, params, resourceParams, optimizerParams, jobJDL, proxyChain)
      print "44"
      if not submission['OK']:
        self.status = 'Failed'
        result = self.setRingStatus()
        self.__report(jobID, 'Failed', submission['Message'])
        self.status = 'Failed'
        updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
        result = self.mpiService.setRingStatus(updDict)
        return self.__finish(submission['Message'])
      #elif 'PayloadFailed' in submission:
        # Do not keep running and do not overwrite the Payload error
      #  print "45"
      #  self.status = 'Failed'
      #  updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
      #  result = self.mpiService.setRingStatus(updDict)
      #  self.__finish('Payload execution failed with error code %s' % submission['PayloadFailed'])
      #  return self.status
      else:
        print "46"
        self.status = 'Running'
        updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
        result = self.mpiService.setRingStatus(updDict)
      self.log.verbose('After %sCE submitJob()' %(self.ceName))
    except Exception, x:
      self.status = 'Failed'
      result = self.setRingStatus()
      self.log.exception()
    ####result = self.__getCPUTimeLeft()
    ####self.log.info('Result from TimeLeft utility:', result)
    return S_OK(self.status)

  #############################################################################
  def __finish(self, message):
    """Force the JobAgent to complete gracefully.
    """
    print "47"
    self.log.info('MPIJobAgent will stop with message "%s", execution complete.' %message)
    fd = open(self.controlDir+'/stop_agent', 'w')
    fd.write('JobAgent Stopped at %s [UTC]' % (time.asctime(time.gmtime())))
    fd.close()
    self.status = 'Failed'
    updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
    result = self.mpiService.setRingStatus(updDict)
    result = self.shutdownRing()
    cmd =('rm -rf %s') % (self.jobPath)
    self.jobPath = False
    self.sharedHome = False
    return S_ERROR('Message')

  #############################################################################
  def finalize(self):
    """Force the JobAgent to complete gracefully.
    """

    print "48"
    gridCE = gConfig.getValue( 'LocalSite/GridCE', 'Unknown' )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.setPilotStatus( str( self.pilotReference ), 'Done', gridCE, 'Report from JobAgent' )
    if not result['OK']:
      self.log.warn( result['Message'] )
    cmd =('rm -rf %s') % (self.jobPath)
    x = commands.getoutput(cmd)
    self.status = False
    self.masterFlag = False
    self.slaveFlag = False
    self.jobPath = False
    self.sharedHome = False
    self.mpiFlavor = False

    return S_OK()
  #############################################################################

  def testStatusRing(self, directory):
    """ If MPI Flavor is MPICH2 this function run mpdtrace command to get the ring status, and know how many processors are
    into the ring
    """
    print "49" 
    if self.mpiFlavor == 'MPICH2':
      result1 = int(commands.getoutput('mpdtrace -l|wc -l'))
      result = int(result1)
      startFlagTime = time.time()
      maxFlagTime = 60
      while result < self.numProcJob:
        comparationTime = time.time() - startFlagTime
        if comparationTime > maxFlagTime:
          result = S_ERROR("Failed by time")
      print "50"
      result1 = int(commands.getoutput('mpdtrace -l|wc -l'))
      result = int(result1)
      startDict = {'JobID':self.jobID, 'RingID':self.ringID}
      result = self.mpiService.startRing(startDict)
      self.status = result['Value']
            

    elif self.mpiFlavor == 'MPICH1':
      print "51" 
      startDict = {'JobID':self.jobID, 'RingID':self.ringID}
      result = self.mpiService.startRing(startDict)
      self.status =  result['Value']
      result={'Status':self.status}
    return S_OK(result)

 #############################################################################

  def __getCPUTimeLeft(self):
    """Wrapper around TimeLeft utility. Returns CPU time left in DIRAC normalized
       units. This value is subsequently used for scheduling further jobs in the
       same slot.
    """
    print "52"
    utime, stime, cutime, cstime, elapsed = os.times()
    cpuTime = utime + stime + cutime
    self.log.info('Current raw CPU time consumed is %s' %cpuTime)
    tl = TimeLeft()
    result = tl.getTimeLeft(cpuTime)
    return result

  #############################################################################

  def __changeProxy(self, oldProxy, newProxy):
    """Can call glexec utility here to set uid or simply log the changeover
       of a proxy.
    """
    print "53"
    self.log.verbose('Log proxy change (to be instrumented)')
    return S_OK()

  #############################################################################

  def __setupProxy(self, job, ownerDN, ownerGroup, workingDir):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """
    print "54"
    self.log.info("Requesting proxy for %s@%s" % (ownerDN, ownerGroup))
    token = gConfig.getValue("/Security/ProxyToken", "")
    if token:
      retVal = gProxyManager.getPayloadProxyFromDIRACGroup(ownerDN,
                                                            ownerGroup,
                                                            token,
                                                            self.defaultProxyLength
                                                          )
    else:
      self.log.info("No token defined. Trying to download proxy without token")
      retVal = gProxyManager.downloadVOMSProxy(ownerDN,
                                                ownerGroup,
                                                limited = True,
                                                requiredTimeLeft = self.defaultProxyLength)
    if not retVal[ 'OK' ]:
      self.log.error('Could not retrieve proxy')
      self.log.verbose(retVal)
      self.__setJobParam(job, 'ProxyError', retVal[ 'Message' ])
      os.system('dirac-proxy-info')
      sys.stdout.flush()
      self.__report(job, 'Failed', 'Proxy Retrieval')
      return self.__finish('Failed Proxy retrieval')
      #####return S_ERROR('Error retrieving proxy')

    chain = retVal[ 'Value' ]

    return S_OK(chain)

  #############################################################################

  def __checkInstallSoftware(self, jobID, jobParams, resourceParams):
    """Checks software requirement of job and whether this is already present
       before installing software locally.
    """
    print "55"
    if not jobParams.has_key('SoftwareDistModule'):
      msg = 'Job has no software installation requirement'
      self.log.verbose(msg)
      return S_OK(msg)

    self.__report(jobID, 'Matched', 'Installing Software')
    softwareDist = jobParams['SoftwareDistModule']
    self.log.verbose('Found VO Software Distribution module: %s' %(softwareDist))
    argumentsDict = {'Job':jobParams, 'CE':resourceParams}
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule(softwareDist, argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    return result

  #############################################################################

  def __submitJob(self, jobID, jobParams, resourceParams, optimizerParams, jobJDL, proxyChain):
    """Submit job to the Computing Element instance after creating a custom
       Job Wrapper with the available job parameters.
    """
    print "56"
    result = self.__createJobWrapper( jobID, jobParams, resourceParams, optimizerParams )

    if not result['OK']:
      return result

    wrapperFile = result['Value']
    self.__report( jobID, 'Matched', 'Submitted To CE' )

    wrapperName = os.path.basename( wrapperFile )
    self.log.info( 'Submitting %s to %sCE' % ( wrapperName, self.ceName ) )

    #Pass proxy to the CE
    proxy = proxyChain.dumpAllToString()
    if not proxy['OK']:
      self.log.error( proxy )
      return self.__finish('Payload proxy not found')   
      #####return S_ERROR( 'Payload Proxy Not Found' )

    payloadProxy = proxy['Value']
    # FIXME: how can we set the batchID before we submit, this makes no sense
    batchID = 'dc%s' % ( jobID )
    #submission = self.computingElement.submitJob( wrapperFile, jobJDL, payloadProxy, batchID )
    submission = self.computingElement.submitJob( wrapperFile, payloadProxy )
    print "VANESSA" 
    print "wrapperFile:", wrapperFile
    print "jobJDL", jobJDL
    print "payloadProxy", payloadProxy
    print "batchID", batchID
    print "------------------------------------"
    print submission

    ret = S_OK( 'Job submitted' )

    if submission['OK']:
      batchID = submission['Value']
      self.log.info( 'Job %s submitted as %s' % ( jobID, batchID ) )
      self.log.verbose( 'Set JobParameter: Local batch ID %s' % ( batchID ) )
      self.__setJobParam( jobID, 'LocalBatchID', str( batchID ) )
      if 'PayloadFailed' in submission:
        ret['PayloadFailed'] = submission['PayloadFailed']
        return ret
      time.sleep( self.jobSubmissionDelay )
    else:
      self.log.error( 'Job submission failed', jobID )
      self.__setJobParam( jobID, 'ErrorMessage', '%s CE Submission Error' % ( self.ceName ) )
      return self.__finish('CE submission error')
      #####return S_ERROR( '%s CE Submission Error: %s' % ( self.ceName, submission['Message'] ) )

    return ret

    self.status = 'Running'
    result = self.setRingStatus()

    return ret
     



  #############################################################################

  def __createJobWrapper( self, jobID, jobParams, resourceParams, optimizerParams ):
    """This method creates a job wrapper filled with the CE and Job parameters
       to executed the job.
    """
    print "57" 
    arguments = {'Job':jobParams,
                 'CE':resourceParams,
                 'Optimizer':optimizerParams}
    self.log.verbose( 'Job arguments are: \n %s' % ( arguments ) )

    workingDir = gConfig.getValue( '/LocalSite/WorkingDirectory', self.siteRoot )
    if not os.path.exists( '%s/job/Wrapper' % ( workingDir ) ):
      try:
        os.makedirs( '%s/job/Wrapper' % ( workingDir ) )
      except Exception, x:
        self.log.exception()
        return self.__finish('Wrapper directory creation failed') 
        #####return S_ERROR( 'Could not create directory for wrapper script' )

    jobWrapperFile = '%s/job/Wrapper/Wrapper_%s' % ( workingDir, jobID )
    if os.path.exists( jobWrapperFile ):
      self.log.verbose( 'Removing existing Job Wrapper for %s' % ( jobID ) )
      os.remove( jobWrapperFile )
    fd = open( self.jobWrapperTemplate, 'r' )
    wrapperTemplate = fd.read()
    fd.close()

    dateStr = time.strftime( "%Y-%m-%d", time.localtime( time.time() ) )
    timeStr = time.strftime( "%H:%M", time.localtime( time.time() ) )
    date_time = '%s %s' % ( dateStr, timeStr )
    signature = __RCSID__
    dPython = sys.executable

    systemConfig = ''
    if jobParams.has_key( 'SystemConfig' ):
      systemConfig = jobParams['SystemConfig']
      self.log.verbose( 'Job system configuration requirement is %s' % ( systemConfig ) )
      if resourceParams.has_key( 'Root' ):
        jobPython = '%s/%s/bin/python' % ( resourceParams['Root'], systemConfig )
        if os.path.exists( jobPython ):
          self.log.verbose( 'Found local python for job:\n%s' % ( jobPython ) )
          dPython = jobPython
        else:
          if systemConfig == 'ANY':
            self.log.verbose( 'Using standard available python %s for job' % ( dPython ) )
          else:
            self.log.warn( 'Job requested python \n%s\n but this is not available locally' % ( jobPython ) )
      else:
                self.log.warn( 'No LocalSite/Root defined' )
    else:
      self.log.warn( 'Job has no system configuration requirement' )

    if not systemConfig or systemConfig == 'ANY':
      systemConfig = gConfig.getValue( '/LocalSite/Architecture', '' )
      if not systemConfig:
        #return S_ERROR( 'Could not establish SystemConfig' )
        self.log.info( 'Could not establish SystemConfig' )

    logLevel = self.defaultLogLevel
    if jobParams.has_key( 'LogLevel' ):
      logLevel = jobParams['LogLevel']
      self.log.info( 'Found Job LogLevel JDL parameter with value: %s' % ( logLevel ) )
    else:
      self.log.info( 'Applying default LogLevel JDL parameter with value: %s' % ( logLevel ) )

    realPythonPath = os.path.realpath( dPython )
    self.log.debug( 'Real python path after resolving links is:' )
    self.log.debug( realPythonPath )
    dPython = realPythonPath

    siteRootPython = self.siteRoot
    self.log.debug( 'DIRACPython is:\n%s' % dPython )
    self.log.debug( 'SiteRootPythonDir is:\n%s' % siteRootPython )
    libDir = '%s/%s/lib' % ( self.siteRoot, platform )
    scriptsDir = '%s/scripts' % ( self.siteRoot )
    wrapperTemplate = wrapperTemplate.replace( "@SIGNATURE@", str( signature ) )
    wrapperTemplate = wrapperTemplate.replace( "@JOBID@", str( jobID ) )
    wrapperTemplate = wrapperTemplate.replace( "@DATESTRING@", str( date_time ) )
    wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
    wrapperTemplate = wrapperTemplate.replace( "@SITEPYTHON@", str( siteRootPython ) )
    wrapper = open ( jobWrapperFile, "w" )
    wrapper.write( wrapperTemplate )
    wrapper.close ()
    jobExeFile = '%s/job/Wrapper/Job%s' % ( workingDir, jobID )
    #jobFileContents = '#!/bin/sh\nexport LD_LIBRARY_PATH=%s:%s:%s:$LD_LIBRARY_PATH\n%s %s -o LogLevel=debug' %(libDir,lib64Dir,usrlibDir,dPython,jobWrapperFile)
    #jobFileContents = '#!/bin/sh\nexport LD_LIBRARY_PATH=%s\n%s %s -o LogLevel=%s' %(libDir,dPython,jobWrapperFile,logLevel)
    jobFileContents = '#!/bin/sh\n%s %s -o LogLevel=%s' % ( dPython, jobWrapperFile, logLevel )
    jobFile = open( jobExeFile, 'w' )
    jobFile.write( jobFileContents )
    jobFile.close()
    #return S_OK(jobWrapperFile)
    return S_OK( jobExeFile )
  #############################################################################

  def __saveJobJDLRequest(self, jobID, jobJDL):
    """Save job JDL local to JobAgent.
    """
    print "58"
    classAdJob = ClassAd(jobJDL)
    classAdJob.insertAttributeString('LocalCE', self.ceName)
    jdlFileName = jobID+'.jdl'
    jdlFile = open(jdlFileName, 'w')
    jdl = classAdJob.asJDL()
    jdlFile.write(jdl)
    jdlFile.close()
    return S_OK(jdlFileName)

  #############################################################################

  def __requestJob(self, resourceJDL):
    """Request a single job from the matcher service.
    """
    print "59"
    try:
      matcher = RPCClient('WorkloadManagement/Matcher', timeout = 600)
      result = matcher.requestJob(resourceJDL)
      return result
    except Exception, x:
      self.log.exception(lException=x)
      return self.__finish('Job request to matcher service failed with exception')
      #return S_ERROR('Job request to matcher service failed with exception')

  #############################################################################

  def __getJDLParameters(self, jdl):

    """Returns a dictionary of JDL parameters.
    """
    print "60"
    try:
      parameters = {}
      if not re.search('\[', jdl):
        jdl = '['+jdl+']'
      classAdJob = ClassAd(jdl)
      paramsDict = classAdJob.contents
      for param, value in paramsDict.items():
        if re.search('{', value):
          self.log.debug('Found list type parameter %s' %(param))
          rawValues = value.replace('{', '').replace('}', '').replace('"', '').split()
          valueList = []
          for val in rawValues:
            if re.search(',$', val):
              valueList.append(val[:-1])
            else:
              valueList.append(val)
          parameters[param] = valueList
        else:
          self.log.debug('Found standard parameter %s' %(param))
          parameters[param]= value.replace('"', '')
      return S_OK(parameters)
    except Exception, x:
      self.log.exception(lException=x)
      return S_ERROR('Exception while extracting JDL parameters for job')

  #############################################################################

  def __report(self, jobID, status, minorStatus):
    """Wraps around setJobStatus of state update client
    """
    print "61"
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobStatus(int(jobID), status, minorStatus, 'JobAgent')
    self.log.verbose('setJobStatus(%s,%s,%s,%s)' %(jobID, status, minorStatus, 'JobAgent'))
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])
    return jobStatus

  #############################################################################

  def __reportPilotInfo(self, jobID):
    """Sends back useful information for the pilotAgentsDB via the WMSAdministrator
       service.
    """
    print "62"
    gridCE = gConfig.getValue('LocalSite/GridCE', 'Unknown')

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    if gridCE != 'Unknown':
      result = wmsAdmin.setJobForPilot(int(jobID), str(self.pilotReference), gridCE)
    else:
      result = wmsAdmin.setJobForPilot(int(jobID), str(self.pilotReference))

    if not result['OK']:
      self.log.warn(result['Message'])

    result = wmsAdmin.setPilotBenchmark(str(self.pilotReference), float(self.cpuFactor))
    if not result['OK']:
      self.log.warn(result['Message'])

    return S_OK()

  #############################################################################

  def __setJobSite(self, jobID, site):
    """Wraps around setJobSite of state update client
    """
    print "63"
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobSite = jobReport.setJobSite(int(jobID), site)
    self.log.verbose('setJobSite(%s,%s)' %(jobID, site))
    if not jobSite['OK']:
      self.log.warn(jobSite['Message'])

    return jobSite

  #############################################################################

  def __setJobParam(self, jobID, name, value):
    """Wraps around setJobParameter of state update client
    """
    print "64"
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobParam = jobReport.setJobParameter(int(jobID), str(name), str(value))
    self.log.verbose('setJobParameter(%s,%s,%s)' %(jobID, name, value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################

  def __finish(self, message):
    """Force the JobAgent to complete gracefully.
    """
    print "65"
    self.log.info( 'JobAgent will stop with message "%s", execution complete.' % message )
    self.am_stopExecution()
    return S_ERROR( message )

  #############################################################################

  def __rescheduleFailedJob( self, jobID, ringID, status, message ):
    """ Reschedule a failed job.
    """ 
    print "66"
    self.log.warn( 'Failure during %s' % ( message ) )

    jobManager = RPCClient( 'WorkloadManagement/JobManager' )
    jobReport = JobReport( int( jobID ), 'JobAgent' )

    #Setting a job parameter does not help since the job will be rescheduled,
    #instead set the status with the cause and then another status showing the
    #reschedule operation.

    jobReport.setJobStatus( 'Failed', message, sendFlag = False )
    jobReport.setApplicationStatus( 'Failed %s ' % message, sendFlag = False )
    jobReport.setJobStatus( minor = 'ReschedulingJob', sendFlag = True )

    self.log.info( 'Job will be rescheduled' )
    result = jobManager.rescheduleJob( jobID )
    if not result['OK']:
      self.log.error( result['Message'] )
      return self.__finish( 'Problem Rescheduling Job' )

    self.log.info( 'Job Rescheduled %s' % ( jobID ) )
    return self.__finish( 'Job Rescheduled' )

  #################################################################################

  def wait(self):
    """ With this function the agent ask to the service for status of the ring, its a loop executed while status remain equal
    """
    print "67"
    getDict = {'RingID': self.ringID, 'JobID': self.jobID}
    status = self.status
    startFlagTime = time.time()
    maxFlagTime = 50
    while status == self.status:
      #if self.status == "RingInit" or self.status == "Ready":
      #  timeStatus = time.time()
      #  comparationTime = timeStatus - startFlagTime
      #  if comparationTime > maxFlagTime:
      #    self.status = "Failed"
      #    result = self.status
      #    return S_ERROR(result)
      result = self.mpiService.getRingStatus(getDict)
      self.status = result['Value']['Status']
      if not result['OK']:
        self.log.warn('Failed to get the ring status')
        self.log.warn(result['Message'])
        self.status = 'Failed'
      else:
        self.status = result['Value']['Status']
    result = self.status
    return S_OK(result)

#################################################################################

  def masterStart(self):
    """ This function start the master daemon
    """
    print "68"
    if self.mpiFlavor == 'MPICH2':
      print "68.1"
      user = commands.getoutput('echo $USER')
      dir = os.path.exists(('/tmp/mpd2.console_%s')%(user))
      if dir == True:
        #x = commands.getoutput('ls -la /tmp/mpd*')
        #self.__rescheduleFailedJob(self.jobID , 'Other mpd daemon is running in the machine')
        #self.status = 'Failed'
        updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
        result = self.mpiService.setRingStatus(updDict)
        Message = 'Problems to start MPICH environment'
        res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
        if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')

        print "RESULT STATUS Other MPD is running", result
        return self.__finish('Other MPD daemon is running in the same machine') 
        #####return S_ERROR("Master Start Fail Other MPD daemon is running in the machine")
      result2 = (commands.getoutput('rehash'))
      result1 = (commands.getoutput('which mpd'))
      #result3 = (commands.getoutput('which mpdtrace'))
      status, output = commands.getstatusoutput("which mpdtrace")
      print status, output
      print "MPDTRACE"
      if status == 0:
        self.log.info("MPDTRACE OK")
      else:
        self.log.error('Failed to find the mpdtrace upppsss') 
        res = self.__rescheduleAndFinalize(self.jobID, self.ringID, self.status, Message)
        if not res['OK']:
             self.log.error('Failed to reschedule the job and finalize the process')

      print "++++++++++++++++++++++++++++++++++++++++++++"
      commands.getoutput('chmod 600 mpd.conf')
      cmd = (result1)
      start = os.spawnl(os.P_NOWAIT,cmd)
      rest = os.system('sleep 5')
      os.system('sleep 5')
      self.port1 = commands.getoutput('mpdtrace -l |cut -d_ -f2 |cut -d" " -f1')
      print self.port1
      self.port = int(self.port1)
      if type(self.port) is int:
         print "69"
         self.log.info("Port is an Integer")
      else:
         print "70" 
         self.log.warn ("Port is not Integer")
         self.status = 'Failed'
         result = self.mpiService.setRingStatus(updDict)
         print "RESULT Port is not integer:", result 
         return self.__finish('Port is not an integer')
         #####return S_ERROR()
      self.master = (commands.getoutput('/bin/hostname -f'))
      result = {'Master':self.master, 'Port':self.port}
      return S_OK(result)

    elif self.mpiFlavor == 'MPICH1':
     print "71"
     testDict = {'JobID': self.jobID, 'RingID': self.ringID}
     getMachineFile = self.mpiService.getMachineFile(testDict)
     machineFile = getMachineFile['Value']['Value']['Value']
     x = commands.getoutput('touch mf')
     x = commands.getoutput('cat mf')
     for item in machineFile:
       print "72"
       v = type(item)
       line=0
       file = open('./mf', 'r').read().split('\n')
       a = file[:line]
       b = file[line:]
       a.append(item)
       open('mf', 'w').write('\n'.join(a + b))
       result = {'Master':self.master, 'Port':0}
     return S_OK(result)

#############################################################################

  def slaveStart(self, slaveDict):
    """ This function start slave daemon using as paramenters master's hostname and port
    """
    print "73" 
    if self.mpiFlavor=="MPICH2":
      print "74"
      master = slaveDict['Master']
      port1 = slaveDict ['Port']
      port = int(port1)
      commands.getoutput('chmod 600 mpd.conf')
      dir = (commands.getoutput('which mpd'))
      cmd = ('%s -h %s -p %s -n &') % (dir, master, port)
      result = os.system(cmd)
      if result != 0:
        print "75"
        self.__rescheduleFailedJob( self.jobID , 'Other mpd daemon is running in the machine' )
        self.status = 'Failed'
        updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
        result = self.mpiService.setRingStatus(updDict)
        #####return S_ERROR("Master Start Fail")
        return self.__finish('Master Start Fail')
      mpdStatus = (commands.getoutput('ps -ef|grep mpd'))
      self.slaveUP = True
      result = ("True")
    elif self.mpiFlavor == 'MPICH1':
      print "76" 
      self.slaveUP = True
    return S_OK(self.slaveUP)

#############################################################################

  def setRingStatus(self):
    """ This function is used to change the ring status
        Status of the ring are: Empty, Accumulating, Ready, Starting, Running, Done, Failed, Exit
    """
    statDict = {'RingID': self.ringID, 'JobID': self.jobID, 'Status': self.status}
    result = self.mpiService.setRingStatus(statDict)
    print "77" 
    if not result['OK']:
      print "78"
      self.log.warn('Failed to set ring status')
      self.log.warn(result['Message'])
      return self.__finish('Failed to set ring status')
      ####return S_ERROR('Error set ring status')
    return S_OK(result)

#############################################################################

  def shutdownRing(self):
    """ Kill all mpd daemons in all the machines of the ring.
    """
    print "79"
    self.log.info (("Shuting down the ring:%s")%(self.ringID))
    dir = (commands.getoutput('which  mpdallexit'))
    cmd = ('%s &')%(dir)
    result = os.system(cmd)
    self.slaveUP = False
    return

#############################################################################

  def shutdownMPD(self):
    """ Kill mpd daemon started by this agent
    """
    print "80"
    self.log.info (("Shuting down the ring mpd:%s")%(self.ringID))
    dir = (commands.getoutput('which mpdallexit'))
    cmd = ('%s &') % (dir)
    result = os.system(cmd)
    self.slaveUP = False
    return S_OK()

#############################################################################

  def testRing(self):
    """ This function is used to ask service if number of processors into the ring is less or equal than required by the job.
    """
    print "81" 
    testDict = {'RingID': self.ringID, 'JobID': self.jobID, 'Status': self.status, 'MasterFlag': masterFlag}
    result = self.mpiService.testRing(testDict)
    if not result['OK']:
      print "82"
      #####return S_ERROR('Test Ring Failed')
      return self.__finish('Test Ring Failed')

    self.numProcJob = result['Value']['NumberOfProcessorsJob']
    self.numProcRing = result['Value']['NumberOfProcessorsRing']
    self.log.info ("-------------------------------------------")
    self.log.info (("First Test Ring: ProcRing: %s ProcJob: %s")% (self.numProcRing, self.numProcJob))
    self.log.info ("-------------------------------------------")
    while self.numProcRing <  self.numProcJob:
      print "83"
      result = self.mpiService.testRing(testDict)
      if not result['OK']:
       print "84" 
       #####return S_ERROR('Test Ring Failed')
       return self.__finish('Test Ring Failed')
      self.numProcRing = result['Value']['NumberOfProcessorsRing']
      self.numProcJob = result['Value']['NumberOfProcessorsJob']
    self.log.info ("-------------------------------------------")
    self.log.info (("Test Ring Result: ProcRing: %s ProcJob: %s")% (self.numProcRing, self.numProcJob))
    self.log.info ("-------------------------------------------")
    return

  #############################################################################

  def MPICH2environment(self):
    """ Set up MPICH2 environment variables
    """
    print "85"
    os.listdir('.')
    x = commands.getoutput('pwd')
    os.environ['PATH'] = (self.directory.strip('"')+os.sep+('mpich2/bin/')+':'+self.directory.strip('"')+os.sep+('mpich2/include/')+':'+'.'+':'+os.environ['PATH'])
    os.environ['LD_LIBRARY_PATH'] = (self.directory.strip('"')+os.sep+('mpich2/lib/'))+':'+(self.directory.strip('"')+os.sep+('mpich2/include/')+':'+os.environ['LD_LIBRARY_PATH'])
    os.environ['MPD_CONF_FILE'] = (self.directory.strip('"')+os.sep+('mpd.conf'))
    os.listdir('.')
    commands.getoutput('chmod 600 mpd.conf')
    commands.getoutput('hash -r')
    rest = commands.getoutput('echo $PATH')
    print rest
    print "MPICH2environment(self):"
    return S_OK()

  #############################################################################

  def MPICH1environment(self):
    """ Set up MPICH1 environment variables
    """
    print "86"
    os.environ['PATH'] = (self.directory.strip('"')+os.sep+('mpich/bin/')+':'+self.directory.strip('"')+os.sep+('mpich/include/')+':'+'.'+':'+os.environ['PATH'])
    os.environ['LD_LIBRARY_PATH'] = (self.directory.strip('"')+os.sep+('mpich/lib/'))+':'+(self.directory.strip('"')+os.sep+('mpich/include/')+':'+os.environ['LD_LIBRARY_PATH'])
    os.environ['MACHINE_FILE'] = (self.directory.strip('"')+os.sep+('mf'))
    commands.getoutput('chmod 600 mf')
    commands.getoutput('chmod 755 mpich/lib/* mpich/include/*')
    commands.getoutput('hash -r')
    return S_OK()

  ############################################################################
  def __rescheduleAndFinalize(self, jobID, ringID, status, Message):
    """ If an error happen the Job must be rescheduled and the Agent must finalize
    """
    message = Message
    print "AQUI MENSAJE:", message
    #self.__rescheduleFailedJob(jobID, ringID, status, message)
    self.status = 'Failed'
    updDict = {'JobID': jobID, 'RingID': ringID, 'Status': status}
    result = self.mpiService.setRingStatus(updDict)
    if not  result['OK']:
      self.log.error(' Problems to set ring status')
    print "RESULT STATUS Other MPD is running", result
    res = self.__rescheduleFailedJob(jobID, ringID, status, message)
    if not res['OK']:
      self.log.error('Failed to reschedule the job')
    self.__finish(message)
    return S_OK()
  #############################################################################
  def __downloadInputSandbox(self, jobID):
    """ Download the input sandbox in the slaves directory 
    """
    print "43"
    #matcherInfo = jobMatch
    #jobAtt = self.jobMatch['JobJDL']
    #jobJDL = ClassAd(jobAtt)
    #jobID = jobJDL.getAttributeInt('JobID')
    #jobMatchJDL = ClassAd(jobAtt)
    #matcherParams = ['JDL', 'DN', 'Group']
    result = self.mpiService.getJobOptParameters(self.jobID)
    matcherInfo = result['Value']
    print "=======================MATCHER INFO ==========================="
    print matcherInfo
    print "==============================================================="

    self.log.verbose('Job Agent execution loop')
    available = self.computingElement.available()
    if not available['OK']:
      self.log.info('Resource is not available')
      self.log.info(available['Message'])
      return self.__finish('CE Not Available')

    self.log.info(available['Value'])

    ceJDL = self.computingElement.getJDL()
    resourceJDL = ceJDL['Value']
    self.log.verbose(resourceJDL)
    start = time.time()
    matchTime = time.time() - start
    self.log.info('MatcherTime = %.2f (s)' %(matchTime))

    matcherParams = ['JDL', 'DN', 'Group']
    for p in matcherParams:
      if not matcherInfo.has_key(p):
        self.__report(jobID, 'Failed', 'Matcher did not return %s' %(p))
        return self.__finish('Matcher Failed1')
      elif not matcherInfo[p]:
        self.__report(jobID, 'Failed', 'Matcher returned null %s' %(p))
        return self.__finish('Matcher Failed2')
      else:
        self.log.verbose('Matcher returned %s = %s ' %(p, matcherInfo[p]))

    jobJDL = matcherInfo['JDL']
    jobGroup = matcherInfo['Group']
    ownerDN = matcherInfo['DN']

    print "AQUI JDL GROUP DN ======================="
    print jobJDL, jobGroup, ownerDN
    print "============================================" 
    optimizerParams = {}
    for key in matcherInfo.keys():
      if not key in matcherParams:
        value = matcherInfo[key]
        optimizerParams[key] = value

    parameters = self.__getJDLParameters(jobJDL)
    if not parameters['OK']:
      self.__report(jobID, 'Failed', 'downloadInputSandbox - Could Not Extract JDL Parameters')
      self.log.warn(parameters['Message'])
      return self.__finish('JDL Problem')

    params = parameters['Value']

    if not params.has_key('JobID'):
      msg = 'downloadInputSandbox - Job has not JobID defined in JDL parameters'
      self.log.warn(msg)
      return S_OK(msg)
    else:
      jobID = params['JobID']

    if not params.has_key('JobType'):
      self.log.warn('downloadInputSandbox - Job has no JobType defined in JDL parameters')
      jobType = 'Unknown'
    else:
      jobType = params['JobType']

    if not params.has_key('SystemConfig'):
      self.log.warn('downloadInputSandbox - Job has no system configuration defined in JDL parameters')
      systemConfig = 'ANY'
    else:
      systemConfig = params['SystemConfig']

    if not params.has_key('MaxCPUTime'):
      self.log.warn('downloadInputSandbox - Job has no CPU requirement defined in JDL parameters')
      jobCPUReqt = 0
    else:
      jobCPUReqt = params['MaxCPUTime']
    print "===================== LOS LOGS =========================================================="
    self.log.info('Received JobID=%s, JobType=%s, SystemConfig=%s' %(jobID, jobType, systemConfig))
    self.log.info('OwnerDN: %s JobGroup: %s' %(ownerDN, jobGroup))

    try:
      self.__setJobParam(jobID, 'MatcherServiceTime', str(matchTime))
      self.__report(jobID, 'Matched', 'Job Received by Agent')
      self.__setJobSite(jobID, self.siteName)
      self.__reportPilotInfo(jobID)
      ret = getProxyInfo(disableVOMS = True)
      if not ret['OK']:
        self.log.error('Invalid Proxy', ret['Message'])
        self.status = 'Failed'
        updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
        result = self.mpiService.setRingStatus(updDict)

        return self.__rescheduleFailedJob(jobID , 'Invalid Proxy')

      proxyChain = ret['Value']['chain']
      if not 'groupProperties' in ret['Value']:
        self.log.error('Invalid Proxy', 'Group has no properties defined')
        return self.__rescheduleFailedJob(jobID , 'Proxy has no group properties defined')

      if Properties.GENERIC_PILOT in ret['Value']['groupProperties']:
        proxyResult = self.__setupProxy(jobID, ownerDN, jobGroup, self.siteRoot)
        if not proxyResult['OK']:
          self.log.error('Invalid Proxy', proxyResult['Message'])
          self.status = 'Failed'
          updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
          result = self.mpiService.setRingStatus(updDict)
          return self.__rescheduleFailedJob(jobID , 'Fail to setup proxy')
        else:
          proxyChain = proxyResult['Value']

      saveJDL = self.__saveJobJDLRequest(jobID, jobJDL)

      resourceParameters = self.__getJDLParameters(resourceJDL)
      if not resourceParameters['OK']:
        return resourceParameters
      resourceParams = resourceParameters['Value']

      software = self.__checkInstallSoftware(jobID, params, resourceParams)
      if not software['OK']:
        self.log.error('Failed to install software for job %s' %(jobID))
        errorMsg = software['Message']
        if not errorMsg:
          errorMsg = 'Failed software installation'
          self.status = 'Failed'
          updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
          result = self.mpiService.setRingStatus(updDict)
        return self.__rescheduleFailedJob(jobID, errorMsg)

      self.log.verbose('Before %sCE submitJob()' %(self.ceName))
      #submission = self.__submitJob(jobID, params, resourceParams, optimizerParams, jobJDL, proxyChain)
      #print "44"
      #if not submission['OK']:
      #  self.status = 'Failed'
      #  result = self.setRingStatus()
      #  self.__report(jobID, 'Failed', submission['Message'])
      #  self.status = 'Failed'
      #  updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
      #  result = self.mpiService.setRingStatus(updDict)
      #  return self.__finish(submission['Message'])
      #elif 'PayloadFailed' in submission:
        # Do not keep running and do not overwrite the Payload error
      #  print "45"
      #  self.status = 'Failed'
      #  updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
      #  result = self.mpiService.setRingStatus(updDict)
      #  self.__finish('Payload execution failed with error code %s' % submission['PayloadFailed'])
      #  return self.status
      #else:
      #  print "46"
      #  self.status = 'Running'
      #  updDict = {'JobID': self.jobID, 'RingID': self.ringID, 'Status': self.status}
      #  result = self.mpiService.setRingStatus(updDict)
      #self.log.verbose('After %sCE submitJob()' %(self.ceName))
      print "====================== AQUI ANTES DE ================================================="
      print self.jobID
      print params
      print resourceParams
      print optimizerParams
      print "======================================================================================" 
      result = self.__createJobWrapper( self.jobID, params, resourceParams, optimizerParams )
      print result
      print "**************************************************************************************" 
      if not result['OK']:
        return result

      wrapperFile = result['Value']
      wrapperName = os.path.basename( wrapperFile )

    except Exception, x:
      self.status = 'Failed'
      result = self.setRingStatus()
      self.log.exception()
    ####result = self.__getCPUTimeLeft()
    ####self.log.info('Result from TimeLeft utility:', result)
    return S_OK(self.status)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
