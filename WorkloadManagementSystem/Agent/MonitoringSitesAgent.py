########################################################################
# $HeadURL$
# File :   MonitoringSitesAgent.py
# Author : HAMAR, V.
########################################################################
"""  MonitoringSitesAgent: This Agent can be used to submit a simple
     user job for each site available in Dirac resources.
"""
__RCSID__ = ""

from DIRAC.Core.Utilities.ModuleFactory                       import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                import ClassAd
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC                                                    import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.Interfaces.API.DiracAdmin                          import DiracAdmin
from DIRAC.FrameworkSystem.Client.NotificationClient          import NotificationClient
import re, os, sys, string, time, shutil, types, commands, re, time, pprint

diracAdmin = DiracAdmin()
jobID = "" 
jobDict = {} 

class MonitoringSitesAgent( AgentModule ):

#########################################################################  
  def initialize( self ):
    """Sets defaults.
    """
    self.am_setOption( 'PollingTime', 86400 )
    self.GridType = self.am_setOption( 'GridType', "EELA" )
    self.am_setOption( 'shifterProxy', 'MonitoringSitesManager')
    self.addressTo = self.am_setOption( 'MailTo', "hamar@cppm.in2p3.fr" )
    self.addressFrom = self.am_setOption( 'MailFrom', "dirac@dirac.eela.if.ufrj.br" )
    self.subject = "Monitoring Site Agent"

    return S_OK()

#########################################################################
  def execute( self ):
    """The MonitoringSitesAgent execution method.
    """
    
    gridType = self.GridType
    result =  self.getAvailableSites(gridType)
    if not result['OK']:
      self.log.error(result['Value'])
      return S_ERROR(result['Value'])
    else:
      self.log.info('Sites retrieved')
      
    sites=result['Value']  
    msg = ('Sites: %s')%(sites)
    self.log.debug(msg)
      
    result = self.submitMonitorJob(sites)
    if not result['OK']:
      self.log.error(result['Value'])
    else:
      jobDict = result['Value']
      self.sendEmail(jobDict)
      self.log.info('Cycle complete')    
    return S_OK()

#########################################################################
  def getAvailableSites( self, gridType ):
    """ Return a site list where the jobs are going to be submitted
    """
    # Get all the sites available
    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator', timeout = 120 )
    result = wmsAdmin.getSiteMask()
    if not result['OK']:
      self.log.error(result['Value'])
      return S_ERROR(result['Value'])
    else:
      msg = result['Value']
      self.log.debug(msg)

    all_sites = result['Value']

    # remove banned sites from totalList
    result = diracAdmin.getBannedSites(gridType) 
    if result['OK']:
      banned_sites = result['Value']
      msg = ('Banned Sites: %s')%(banned_sites)
      self.log.debug(msg)
    else:
      self.log.error(result['Value'])
      return S_ERROR(result['Value'])

    sites = list(set(all_sites) - set(banned_sites))
    self.log.info(sites)

    return S_OK(sites) 
    

#########################################################################
  def submitMonitorJob(self,sites):
    """ Function used to submit monitor jobs to all sites 
    """
    for site in sites:
        
      fileName = ("site_%s.jdl")%(site)
      fileToWrite = open ( fileName, 'w' )
      jdl = ("""JobName    = "MonitorJob_%s";
                Executable = "/bin/hostname";
                Arguments = "-f";
                StdOutput  = "StdOut";
                StdError   = "StdErr";
                OutputSandbox = {"StdOut","StdErr"};
                Site = "%s";""")%(site,site)

      fileToWrite.write(jdl)
      fileToWrite.close()
      msg=("JDL %s")%(jdl)
      self.log.debug(msg)
      cmd = ('dirac-wms-job-submit %s')% (fileName)
      status, result = commands.getstatusoutput(cmd)
      if status == 0:
         self.log.debug(result)
         cmd = ('echo %s|cut -d= -f 2')%(result)
         jobID = int(commands.getoutput(cmd))
         jobDict[site] = jobID
      else:
         msg = ('Job submission failed: %s')%(result) 
         self.log.error(msg) 
         S_ERROR(msg)
      cmd = ("rm %s")% fileName
      result = commands.getoutput(cmd)
    return S_OK(jobDict)
#########################################################################
  def sendEmail(self, jobDict):
    """ Function than submit an e-mail with sites and respective jobID
    """
    body = ('Sites and JobIDs %s') % (jobDict)
    self.log.debug(body)
    result = NotificationClient().sendMail( self.addressTo, self.subject, body, self.addressFrom)
    if not result['OK']:
      self.log.error("Failed to sent the email")
    else:
      self.log.debug("Email sent")
    return S_OK()
