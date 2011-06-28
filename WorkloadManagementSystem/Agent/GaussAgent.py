########################################################################
# $HeadURL$
# File :   GaussAgent.py
# Author : HAMAR, V.
########################################################################
"""  The GaussAgent is in charge to submit user jobs who required 
     Gaussian application
     - The gaussian software is going to be deployed each time in the
       WNs after that is going to be removed totally in order to prevent
       than other user can make use of it. This software is stored in a
       grid SE under the user than has permission to run.
     - Two other files are necessary:
       - gauss.sh correspond to the script to prepare the env and to 
         execute the slow.sh script (this script is going to download,
         execute gaussian and to send the output files to mylims server)
       - limsProcess.tgz is a set of scripts to be used depending of the 
         gaussian job type.
     GaussManager must be configured in the CS
       Operations/GaussManager/user
       Operations/GaussManager/group
     Options required by the agent:
       GenericLFNGaussSoft = LFN where the software is stored
       NumberOfGaussianJobs = Number of gaussians jobs to be submitted in
                              each cycle
       PollingTime = Time between each agent execution
"""
__RCSID__ = ""

from DIRAC.Core.Utilities.ModuleFactory                       import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                import ClassAd
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC                                                    import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                               import RPCClient
import re, os, sys, string, time, shutil, types, commands, re, time, pprint


class GaussAgent( AgentModule ):

#############################################################################
  def initialize( self ):
    """Sets defaults.
    """
    self.am_setOption( 'PollingTime', 480 )
    self.numberOfGaussianJobs = self.am_getOption( 'NumberOfGaussianJobs', 2 )
    self.genericLFNGaussSoft = self.am_getOption( 'GenericLFNGaussSoft', 'Unknown' )
    self.am_setOption('shifterProxy', 'GaussManager')

    return S_OK()
#############################################################################
  def execute( self ):
    """The GaussAgent execution method.
    """
            
    gaussLFN = self.genericLFNGaussSoft
    numberGaussJobs = self.numberOfGaussianJobs

    result = self.checkGaussianJobs()
    if not result['OK']:
      self.log.error(result['Message'])
      return S_ERROR()
    else:
      numberGaussJobs = int(result['Value'])
      msg = ('Number of pending gaussian jobs: %s')%(numberGaussJobs)
      self.log.info(msg)   
    if numberGaussJobs > 0:
      result = self.submitGaussJob(gaussLFN,numberGaussJobs)
      if not result['OK']:
        self.log.error(result['Message'])
      else:
        msg = ('Number of gaussian jobs submitted: %s')%(numberGaussJobs)
        self.log.info('msg')
    self.log.info('Cycle complete')    
    return S_OK()
  
#############################################################################  
  def submitGaussJob(self,gaussLFN,numberGaussJobs):
    """ Function used to submit gaussian jobs, input sandbox scripts 
        gaussian.sh and limsProcess.tgz 
        suppose to be in WorkloadManagementSystem scripts directory
        the gaussian software must be stored in a SE and the respective
        LFN must be configured in the CS
    """
    
    fileName = ("gauss.jdl")
    fileToWrite = open ( fileName, 'w' )
    jdl = ("""JobName        = "GaussianJob";
              Executable     = "gaussian.sh";
              StdOutput      = "gaussian.out";
              StdError       = "gaussian.err";
              InputSandbox   = {"/opt/dirac/pro/EELADIRAC/WorkloadManagementSystem/scripts/gaussian.sh", %s ,"/opt/dirac/pro/EELADIRAC/WorkloadManagementSystem/scripts/limsProcess.tgz"};
              OutputSandbox  = {"gaussian.out","gaussian.err","test003.log","out.out"};  """)%(gaussLFN)
    fileToWrite.write(jdl)
    fileToWrite.close()
    msg=("JDL %s")%(jdl)
    self.log.debug(msg)
    cmd = ('dirac-wms-job-submit %s')% (fileName)
    counter = 0
    for i in range(numberGaussJobs):
      counter += 1
      status, result = commands.getstatusoutput(cmd)
      if status == 0:
         self.log.debug(result)
      else:
         msg = ('Job submission failed: %s')%(result) 
         self.log.error(msg) 
         S_ERROR(msg)
    cmd = ("rm %s")% fileName
    result = commands.getoutput(cmd)
    msg = ('%s user gaussian jobs submitted')%(numberGaussJobs) 
    self.log.info(msg)
    return S_OK()
    
#############################################################################
  def checkGaussianJobs(self):
    """ This functions contact the lims server in order to get the pending
        number of gaussian user jobs.
    """
    processTypes = ['gaussian_b3lyp_geom_6-31Gs_nmr_6-311Gss','gaussian_b3lyp_geom_6-31Gs_nmr_6-311++Gss','gaussian_b3lyp_geom_6-31Gs_nmr_6-31++Gss','gaussian_b3lyp_geom_6-31Gss_nmr_6-31++Gss']
    numberGaussJobs = 0 
    for process in processTypes:
      limScript = ("""curl -s 'http://clo1v1.mylims.org/lims/Lims?action=process.GetWaitingProcess&email=gaussian_09@nmrdb.org&password=3hhLu6SQ2uIuGeZoHePZPw==&type=%s'|tr -d '\r\n'""")%(process)
      status, result = commands.getstatusoutput(limScript)
      if status==0:
        pendingJobs = int(result)
        self.log.debug(pendingJobs)  
        numberGaussJobs += pendingJobs
      else:
        msg = ('Failed to get pending gaussian jobs: %s')%(result)
        self.log.error(msg)
        return S_ERROR()
    msg = ('Total number pending jobs: %s')%(numberGaussJobs)
    self.log.info(msg)
    return S_OK(numberGaussJobs)

#############################################################################
