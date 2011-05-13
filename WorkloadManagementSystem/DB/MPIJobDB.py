########################################################################
# $Header: $
########################################################################

""" DIRAC MPIJobDB class is a front-end to the MPI part of the WMS Job database

    The following methods are provided for public usage:

    createR(createDict)
    selectR(matchDict)
    setRStatus(statDict)
    testR(testDict)
    getRAttributes(ringID)
    addHostToR(addDict)
    updProc(updDict)
    getRStatus(getDict)
    updateR(updDict)


"""

__RCSID__ = "$Id: $"

import re, os, sys, string, types
import time, datetime, operator

from types                       		import *
from DIRAC                           		import gConfig, gLogger, S_OK, S_ERROR, Time
from DIRAC.Core.Base.DB              		import DB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight	import ClassAd

DEBUG = 1
if DEBUG:
  debugFile = open( "MPIJobDB.debug.log", "w" )

#############################################################################
class MPIJobDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Standard Constructor
    """
    DB.__init__(self,'MPIJobDB','WorkloadManagement/MPIJobDB',maxQueueSize)

#############################################################################

  def selectRings(self,condDict):
    """ Select Rings according to the given selection criteria
        Inputs: Match Dictionary {Site,CE,Platform}
        Output: Ring List
    """
    print "DB1"
    condition = self.buildCondition(condDict)
    req = "SELECT RingID,Status,JobID FROM Rings %s" % condition
    result = self._query(req)
    if not result['OK']:
      print "DB2"
      return S_OK(result)
    value = result['Value']
    ringList = list(value)
    print ringList
    return S_OK(ringList)

#############################################################################

  def _query( self, cmd, conn=False ):
    """ Make queries to MPIJob DB
    """
    print "DB3"
    start = Time.time()
    ret = DB._query( self, cmd, conn )
    if DEBUG:
      print >> debugFile, Time.time() - start, cmd.replace('\n','')
      debugFile.flush()
    print ret
    return ret

#############################################################################

  def _update( self, cmd, conn=False ):
    """ Update MPIJob Database
    """ 
    print "DB4"
    start = Time.time()
    ret = DB._update( self, cmd, conn )
    if DEBUG:
      print >> debugFile, Time.time() - start, cmd.replace('\n','')
      debugFile.flush()
    print ret
    return ret

#############################################################################

  def createRing(self, createDict):
    """ Insert the initial attributes into the Rings table,
        Set Initial Ring Attributes and Status,
        Insert the mpiPilot as master
        Input: createDict = {JobID, Status, Site, CE, Platform, Master, Port,
                             NumberOfProcessorsJob, Flavor}
        Output: result = {RingID, Status, JobID}
    """
    print "DB5"
    ringAttrNames  = []
    ringAttrValues = []

    jobID = createDict['JobID']
    for i,j in createDict.iteritems():
      ringAttrNames.append(i)
      ringAttrValues.append(j)

    ringAttrNames.append('ExecutionTime')
    ringAttrValues.append('')
    ringAttrNames.append('TimeNew')
    ringAttrValues.append('')

    res = self._getConnection()
    if not res['OK']:
      return S_ERROR( '0 %s\n%s' % (err, res['Message'] ) )
    connection = res['Value']

    result = self._insert( 'Rings', ringAttrNames, ringAttrValues, connection)
    if not result['OK']:
      print "DB6"
      return S_ERROR(result)

    if not res['OK']:
      connection.close()
      return S_ERROR( '1 %s\n%s' % (err, res['Message'] ) )

    cmd = 'SELECT LAST_INSERT_ID()'
    res = self._query( cmd, connection )
    if not res['OK']:
      connection.close()
      return S_ERROR( '2 %s\n%s' % (err, res['Message'] ) )
    ringID = res['Value'][0][0]

    cmd = 'UPDATE Rings SET LastTimeUpdate=UTC_TIMESTAMP(),TimeNew=UTC_TIMESTAMP() WHERE RingID=%s AND JobID=%s' % (ringID, jobID)
    result1 = self._update(cmd)

    dict = {'RingID': ringID,'JobID':jobID}
    result = self.selectRings(dict)
    values = result['Value']
    result ={}
    keys = ['RingID', 'Status', 'JobID']
    for x,y,t in values:
          z = int(str(x).strip('L'))
          v = int(str(t).strip('L'))
          result.setdefault('RingID',z)
          result.setdefault('Status',y)
          result.setdefault('JobID',v)
    print result
    return (result)

################################################################################

  def matchStatusRing(self, matchDict):
    """ Return Ring Attributes (ID, status and JobID) if exist a ring in the
        site where the pilot is running, or default values if is not a ring in 
        the site  
        Input:{RingID, JobID}
        Output: {RingID, JobID, Status}
    """
    print "DB7"
    result = self.selectRings(matchDict)
    if not result['OK']:
      print "DB8"
      return S_ERROR('Message')
    elif result['Value'] == []:
        print "DB9"
        result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
        return S_OK(result)
    else:
        values = result['Value']
        result ={}
        keys = ['RingID', 'Status']
        for x,y,u in values:
            z = int(str(x).strip('L'))
            v = int(str(u).strip('L'))
            result.setdefault('RingID',z)
            result.setdefault('Status',y)
            result.setdefault('JobID',v)
    print result
    return S_OK(result)


#############################################################################

  def selectRing(self, matchDict):
    """ Select the ring of the site and return ring attributes.
        If the match return an empty value the default values are asigned to
        the variables.
        Input: {Site, Platform, CE}
        Output: {RingID, Status, JobID}
    """
    print "DB10"
    result = self.selectRings(matchDict)
    print "VH result select Rings", result
    if not result['OK']:
      print "DB11"
      return S_ERROR()
    elif result['Value'] == []:
      print "DB12"
      result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
      return S_OK(result)
    else:
      print "DB12"
      print "VH >>>>>>>>> DB12",result
      pass 
    values = result['Value']
    result ={}
    keys = ['RingID', 'Status', 'JobID']
    for x,y,t in values:
      if y == 'Accumulating' or y == 'New':
          z = int(str(x).strip('L'))
          v = int(str(t).strip('L'))
          result.setdefault('RingID',z)
          result.setdefault('Status',y)
          result.setdefault('JobID',v)
          jobID=result['JobID']
          ringID=result['RingID']
          matchDict={'JobID':jobID,'RingID':ringID}
          print "VH >>>>>>>>>>>>",matchDict
          comparation=self.numProcComparation(matchDict)
          print "COMPARATION >>>>>>>>>>>>>>", comparation
          if comparation==False:
            print "FALSA"
            result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
          else:
            print comparation
    if result == {}:
      print "DB13"
      result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
    print "SALIENDO DE LA COMPARACION EL RESULTADO ES:", result
    return S_OK(result)

#############################################################################

  def matchMaster(self, hostname):
    """ If exist a master in the same host the pilot must continue with
        normal jobs, it is not recommendable to have 2 masters in the same  
        machine
        Input: hostname
        Output: result (OK or NO)
    """
    print "DB14"
    req = "SELECT * FROM Rings WHERE Master='%s' AND (Status='Running' OR Status='Ready' OR Status='Starting')" %  hostname
    result = self._query(req)
    if len(result['Value'])<>0:
      result='NO'
    else:
      result='OK'
    print result
    return S_OK(result)

#############################################################################

  def matchPilot(self, matchDict):
    """ Match the pilot with existing site ring, return pilot status 
        Input: matchDict
        Output: {PilotID, Status}
    """
    print "DB15"
    ringID = matchDict['RingID']
    jobID = matchDict['JobID']
    pilotID = matchDict['PilotID']
    req = "SELECT PilotID,Status FROM MPIPilots WHERE RingID=%s, JobID=%s, PilotID=%s" % ringID,jobID,pilotID
    result = self._query(req)
    if not result['OK']:
      print "DB16"
      return S_OK(result)
    value = result['Value']
    ringList = list(value)
    values = result['Value']
    result ={}
    keys = ['PilotID', 'Status']
    for x,y in values:
          z = int(str(x).strip('L'))
          result.setdefault('PilotID',z)
          result.setdefault('Status',y)
    print result
    return S_OK(result)

#############################################################################

  def setRingStatus(self, statDict):
    """ Update ring status and time update.
        Input: statDict = {JobID, RingID, Status}
        Output: result = {JobID, RingID}
    """ 
    print "DB17"
    status = statDict['Status']
    ringID = statDict['RingID']
    jobID = statDict['JobID']
    cmd = 'UPDATE Rings SET Status=\'%s\', LastTimeUpdate=UTC_TIMESTAMP() WHERE RingID=%s AND JobID=%s' % (status,ringID,jobID)
    result = self._update(cmd)
    matchDict = {'RingID':ringID,'JobID':jobID}
    result = self.matchStatusRing(matchDict)
    print result
    return S_OK(result)

#############################################################################

  def testRing(self, testDict):
    """ Return Number of Job Processors than are required and Number of 
        Processors into ring 
        Input:  testDict {RingID, JobID, Status, MasterFlag}
        Output: result {NumberOfProcessorsJob, NumberOfProcessorsRing}
    """ 
    print "DB18"
    ringID = testDict['RingID']
    jobID = testDict['JobID']
    cmd = 'SELECT NumberOfProcessorsJob, NumberOfProcessorsRing FROM Rings WHERE RingID=%s AND JobID=%s' % (ringID,jobID)
    result1 = self._query(cmd)
    values = result1['Value']
    result ={}
    for x,y in values:
      z = int(str(x).strip('L'))
      t = int(str(y).strip('L'))
      result.setdefault('NumberOfProcessorsJob',z)
      result.setdefault('NumberOfProcessorsRing',t)
    print "VH RESULT test RING",result
    return S_OK(result)

###################################################################################

  def getRingAttributes(self, attDict):
    """ Return RingID, Master and Port from a particular ring 
        Inputs: attDict = {JobID, RingID}
        Output: result { RingID, JobID, Master, Port}
    """ 
    print "DB19"
    ringID = attDict['RingID']
    jobID = attDict['JobID']
    cmd = 'SELECT RingID,JobID,Master,Port FROM Rings WHERE RingID=%s AND JobID=%s' % (ringID,jobID)
    result1 = self._query(cmd)
    values = result1 ['Value']
    result = {}
    for v,w,x,y in values:
      z = int(str(v).strip('L'))
      t = int(str(w).strip('L'))
      p = int(str(y).strip('L'))
      result.setdefault('RingID',z)
      result.setdefault('JobID',t)
      result.setdefault('Master',x)
      result.setdefault('Port',p)
    print result
    return S_OK(result)

###################################################################################

  def addHostToRing(self, addDict):
    """ Insert new host information into MPIJob DB for a particular ring. Returns
        PilotID
        Input: addDict = {RingID, JobID, PilotType, Status, Hostname,
                          ResourceJDL, PilotJobReference, Rank}
        Output: PilotID
    """
    print "DB20"
    mpiPilotAttrNames  = []
    mpiPilotAttrValues = []
    ringID = addDict['RingID']
    jobID = addDict['JobID']
    updDict = {'RingID': ringID, 'JobID': jobID}
    for i,j in addDict.iteritems():
      mpiPilotAttrNames.append(i)
      mpiPilotAttrValues.append(j)

    res = self._getConnection()
    if not res['OK']:
      return S_ERROR( '0 %s\n%s' % (err, res['Message'] ) )
    connection = res['Value']
    result = self._insert('MPIPilots', mpiPilotAttrNames, mpiPilotAttrValues, connection)
    if not res['OK']:
      print "DB21"
      connection.close()
      return S_ERROR( '1 %s\n%s' % (err, res['Message'] ) )

    cmd = 'SELECT LAST_INSERT_ID()'
    res = self._query( cmd, connection )
    if not res['OK']:
      connection.close()
      return S_ERROR( '2 %s\n%s' % (err, res['Message'] ) )

    try:
      connection.close()
      pilotID = int(res['Value'][0][0])
      self.log.info( 'MPIPilotDB: New PilotID served "%s"' % pilotID )
    except Exception, x:
      return S_ERROR( '3 %s\n%s' % (err, str(x) ) )

    result = self.updateProcessors(updDict)
    print "DB22"
    print pilotID
    return  S_OK(pilotID)

###################################################################################

  def updateProcessors(self, updDict):
    """ Update number of ring processors than are part of particular ring. 
        Input: {RingID, JobID}
        Output:{RingID}
    """ 
    print "DB23"
    ringID = updDict['RingID']
    jobID = updDict['JobID']
    req = ('SELECT NumberOfProcessorsRing, NumberOfProcessorsJob FROM Rings WHERE RingID=%s AND JobID=%s') % (ringID,jobID)
    result = self._query(req)
    if not result['OK']:
      print "DB24"
      return S_OK(result)
    value ={}
    temp = result['Value']
    for x,y in temp:
      v = temp[0]
      z = int(str(x).strip('L'))
      value.setdefault('numProce',z)
      value.setdefault('numProceJ',y)

    numProc=int(value['numProce'])+1
    timeUpd = Time.time()
    cmd = 'UPDATE Rings SET NumberOfProcessorsRing=%s, LastTimeUpdate=UTC_TIMESTAMP() WHERE RingID=%s AND JobID=%s' % (numProc, ringID,jobID)
    result = self._update(cmd)
    print "RESULT SELF UPDATE", result
    if not result['OK']:
      print "Result no OK", result
      print "DB25"
      return S_ERROR(result['Message'])
    matchDict = {'RingID':ringID}
    result = self.selectRing(matchDict)
    #result = ringID
    print "VH >>>>>>>>>>>>>  ELIMINE", result
    return S_OK(result)

###################################################################################

  def getRingStatus(self, getDict):
    """ Returns ring status
        Inputs : getDict{
        Output: result{RingID, Status, JobID}
    """
    print "DB25"
    result = self.selectRings(getDict)
    values = result['Value']
    result ={}
    keys = ['RingID', 'Status', 'JobID']
    for x,y,t in values:
      z = int(str(x).strip('L'))
      v = int(str(t).strip('L'))
      result.setdefault('RingID',z)
      result.setdefault('Status',y)
      result.setdefault('JobID',v)
    print result
    return S_OK(result)

###################################################################################

  def updateRing(self,updDict):
    """ Update Ring port and status attributes after master of MPICH2 starts
        Inputs: {Port, RingID, JobID}
        Output: {RingID, Status, JobID}
    """ 
    print "DB15"
    port = updDict['Port']
    ringID = updDict['RingID']
    jobID = updDict['JobID']
    status = 'RingInit'
    timeUpd = Time.time()
    req = "UPDATE Rings SET Port=%s, LastTimeUpdate=UTC_TIMESTAMP(), Status=\'%s\' WHERE RingID=%s AND JobID=%s" % (port,status,ringID,jobID)
    result = self._query(req)
    if not result['OK']:
      print "DB16"
      self.log.info ('UPDATE PORT ERROR')
      return S_OK(result)
    dict = {'RingID': ringID, 'JobID': jobID}
    result = self.selectRings(dict)
    values = result['Value']
    result ={}
    keys = ['RingID', 'Status', 'JobID']
    for x,y,t in values:
          z = int(str(x).strip('L'))
          v = int(str(t).strip('L'))
          result.setdefault('RingID',z)
          result.setdefault('Status',y)
          result.setdefault('JobID',v)
    print result
    return S_OK(result)

###################################################################################

  def machineFile(self,getDict):
    """ Select machine files according to the given selection criteria
        Input: RingID, JobID
        Output: RingList
    """
    print "DB17"
    jobID = getDict['JobID']
    ringID = getDict['RingID']
    req = "SELECT Hostname FROM MPIPilots WHERE RingID=%s and JobID=%s" % (ringID,jobID)
    result = self._query(req)
    if not result['OK']:
      print "DB18"
      return S_ERROR(result)
    value = result['Value']
    ringList = [x[0]  for x in value]
    print ringList
    return S_OK(ringList)

################################################################################
  
  def getMPIFlavor(self,getDict):
    """ Select flavor from specific Ring
        Input: addDict {RingID, JobID, PilotType, Status, Hostname, ResourceJDL,
                        PilotJobReference, Rank}
        Output: {MPIFlavor, JobID}  
    """
    print "DB18"
    jobID = getDict['JobID']
    ringID = getDict['RingID']
    req = "SELECT Flavor,JobID FROM Rings WHERE RingID=%s and JobID=%s" % (ringID,jobID)
    result1 = self._query(req)
    if not result1['OK']:
      print "DB19"
      return S_ERROR(result)
    values = result1 ['Value']
    result = {}
    for v,y in values:
      z = (str(v).strip('L'))
      p = int(str(y).strip('L'))
      result.setdefault('MPIFlavor',z)
      result.setdefault('JobID',p)
    print result
    return result
################################################################################
  def numProcComparation(self,compDict):
    """ Compare number processors of the job and number of processors in the ring
        Input: compDict = {JobID,RingID}
        Output: result (True or False)
    """
    print "DB20"
    jobID = compDict['JobID']
    ringID = compDict['RingID']
    result=self.testRing(compDict) 
    print "---------------------------------------------"
    print "RESULT COMPARATION TEST RING", result
    print "---------------------------------------------"
    if result['OK']:
       print "Result OK"
       numberOfProcessorsJob=int(result['Value']['NumberOfProcessorsJob'])
       numberOfProcessorsRing=int(result['Value']['NumberOfProcessorsRing'])
       if int(numberOfProcessorsJob) > int(numberOfProcessorsRing):
         print "Se necesita el proc en el ring"
         result = True
       elif [numberOfProcessorsJob < numberOfProcessorsRing] or [numberOfProcessorsJob == numberOfProcessorsRing]:
         print "Se debe empezar un anillo nuevo"
         result = False
       else:
         print "No se porque es vacio"
         result = False
    print result
    return result
#############################################################################
#############################################################################
  def selectRing1(self, matchDict):
    """ Select the ring of the site and return ring attributes.
        If the match return an empty value the default values are asigned to
        the variables.
    """
    print "DB10"
    result = self.selectRings1(matchDict)
    print "VH result select Rings", result
    if not result['OK']:
      print "DB11"
      return S_ERROR()
    elif result['Value'] == []:
      print "DB12"
      result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
      return S_OK(result)
    else:
      print "DB12"
      print "VH >>>>>>>>> DB12",result
      pass
    values = result['Value']
    result ={}
    keys = ['RingID', 'Status', 'JobID']
    for x,y,t in values:
      if y == 'Accumulating' or y == 'New':
          z = int(str(x).strip('L'))
          v = int(str(t).strip('L'))
          result.setdefault('RingID',z)
          result.setdefault('Status',y)
          result.setdefault('JobID',v)
          jobID=result['JobID']
          ringID=result['RingID']
          matchDict={'JobID':jobID,'RingID':ringID}
          print "VH >>>>>>>>>>>>",matchDict
          comparation=self.numProcComparation(matchDict)
          print "COMPARATION >>>>>>>>>>>>>>", comparation
          if comparation==False:
            print "FALSA"
            result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
          else:
            print comparation
    if result == {}:
      print "DB13"
      result = {'RingID': 0, 'Status': 'Empty', 'JobID': 0}
    print result
    return S_OK(result)

#############################################################################

  def selectRings1(self,condDict):
    """ Select Rings according to the given selection criteria
    """
    print "DB1"
    condition = self.buildCondition(condDict)
    print "---------------------------------------"
    print condition
    print "---------------------------------------"
    req = "SELECT RingID,Status,JobID FROM Rings %s AND NumberOfProcessorsJob > NumberOfProcessorsRing" % condition
    print "VH >>>>>>>>>>>>> REQ", req
    result = self._query(req)
    print "============================================"
    print "============================================"
    print result
    print "============================================"
    print "============================================"
    if not result['OK']:
      print "DB2"
      return S_OK(result)
    value = result['Value']
    ringList = list(value)
    print ringList
    return S_OK(ringList)

#############################################################################

#############################################################################
#############################################################################
  def selectRingsWeb(self, condDict, older=None, newer=None, timeStamp='LastUpdateTime', orderAttribute=None, limit=None ):
    """ Select rings matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by RingID if requested, the result is limited to a given
        number of rings if requested.
    """
    print "CONDITION DICTIONARY SELECT RING WEBDB", condDict, "*************************************************"
    self.log.debug( 'MPIJobDB.selectRings: retrieving rings.' )
    print condDict, "<<<<<<<<<<<<<<<<<<<<<<<<<<< DB CONDITION DICTIONARY"
    condition = self.buildCondition(condDict, older, newer, timeStamp)

    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderType = orderAttribute.split(':')[1].upper()
        orderField = orderAttribute.split(':')[0]
      condition = condition + ' ORDER BY ' + orderField
      print condition
      if orderType:
        condition = condition + ' ' + orderType

    if limit:
      condition = condition + ' LIMIT ' + str(limit)
    
    cmd = 'SELECT RingID from Rings ' + condition
    print cmd, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<VHAMAR"
    res = self._query( cmd )
    print "SELECT RINGS WEB", res
    if not res['OK']:
      return res

    if not len(res['Value']):
      return S_OK([])
    result = map( self._to_value, res['Value'] )
    return S_OK( map( self._to_value, res['Value'] ) )
    values = res['Value']
    print "values", values
    print "+++++++++++++++++++++++++++++++++++++"
    #result = [] 
    #for x,y in values:
    #        print x
    #        print y
    #        z = int(str(x).strip('L'))
    #        v = int(str(y).strip('L'))
    #        a = [] 
    #        a.append(z)
    #        a.append(v)
    #        print a
    #        result.append(a)
    #        print result
    #print "AQUI EL NUEVO RESULT", result
    #return S_OK(result)
#############################################################################
  def getAttributesForRingList(self,ringIDList,attrList=[]):
    """ Get attributes for the ring in the the ringIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[ringID][attribute_name] = attribute_value
    """
    if not ringIDList:
      return S_OK({})
    if attrList:
      attrNames = string.join(map(lambda x: str(x),attrList ),',')
      attr_tmp_list = attrList
    else:
      attrNames = string.join(map(lambda x: str(x),self.ringAttributeNames),',')
      attr_tmp_list = self.ringAttributeNames
    ringList = string.join(map(lambda x: str(x),ringIDList),',')

    # FIXME: need to check if the attributes are in the list of job Attributes

    cmd = 'SELECT RingID,%s FROM Rings WHERE RingID in ( %s )' % ( attrNames, ringList )
    res = self._query( cmd )
    if not res['OK']:
      return res
    try:
      retDict = {}
      for retValues in res['Value']:
        ringID = retValues[0]
        ringDict = {}
        ringDict[ 'RingID' ] = ringID
        attrValues = retValues[1:]
        for i in range(len(attr_tmp_list)):
          try:
            ringDict[attr_tmp_list[i]] = attrValues[i].tostring()
          except:
            ringDict[attr_tmp_list[i]] = str(attrValues[i])
        retDict[int(ringID)] = ringDict
      return S_OK( retDict )
    except Exception,x:
      return S_ERROR( 'MPIJobDB.getAttributesForRingList: Failed\n%s'  % str(x) )

#############################################################################
  def getDistinctRingAttributes(self,attribute, condDict = {}, older = None, newer=None, timeStamp='LastUpdateTime'):
    """ Get distinct values of the job attribute under specified conditions
    """
    cmd = 'SELECT  DISTINCT(%s) FROM Rings ORDER BY %s' % (attribute,attribute)

    cond = self.buildCondition( condDict, older=older, newer=newer, timeStamp=timeStamp )

    result = self._query( cmd + cond )
    if not result['OK']:
      return result

    attr_list = [ x[0] for x in result['Value'] ]
    print "DISTINCT RING ATTRIBUTES", attr_list
    return S_OK(attr_list)
#############################################################################
  def killRing(ring):
    """ Kill the ring
    """
    cmd = ("UPDATE Rings SET Status='FAILED' WHERE RingID=%s") % (ringID)
    result = self._update(cmd)
    print result
    return result
