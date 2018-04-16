#!/usr/bin/env python

import os,sys
import datetime

sys.path.append('/u/ec/dragon/lsst/DC2mon/tabulate-0.8.2')
import tabulate

debug = False

taskList=[
          'DC2-R1-2p-WFD-u',
          'DC2-R1-2p-WFD-g',
          'DC2-R1-2p-WFD-r',
          'DC2-R1-2p-WFD-i',
          'DC2-R1-2p-WFD-z',
          'DC2-R1-2p-WFD-y',
          'DC2-R1-2p-uDDF-u',
          'DC2-R1-2p-uDDF-g',
          'DC2-R1-2p-uDDF-r',
          'DC2-R1-2p-uDDF-i',
          'DC2-R1-2p-uDDF-z',
          'DC2-R1-2p-uDDF-y']

taskSteps = ['setupVisit','RunTrim','RunRaytrace','finishVisit']


## Combine certain Pipline II states into something simpler...
#####failed = ['FAILED','TERMINATED','CANCELED','CANCELING']
failed = ['FAILED','CANCELED','CANCELING']
pending = ['READY','QUEUED','SUBMITTED']

queryFile = '/u/ec/dragon/lsst/DC2mon/query.sql'

startTime = datetime.datetime.now()


## PART I:  Acquire pipeline data from Oracle
##############################################################
def query(taskname):
   #print '\nAcquire data from Pipeline database'
# You should have sourced your ~srs/oracle/bin/setup-11g-j7.[c]sh file already
   sys.path.insert(0,"/afs/slac.stanford.edu/package/python/TWW/lib/python2.7/site-packages")
   import cx_Oracle
   import json

   url = "@pipeline-prod"
   conn = cx_Oracle.connect(url)
   curs = conn.cursor()

   with open(queryFile) as sqlf:
      sql = sqlf.read()
      pass

   sql = sql[:sql.find(";")]  # Don't forget to trim ';' for cx_oracle
   curs.execute(sql,task_name=taskname)

   colnames = [desc[0] for desc in curs.description]

   rows = []
   for row in curs.fetchall():
      rows.append(row)
      pass
   conn.close()
   return (colnames,rows)

## Returns 34 columns
## colnames =  ['TASK', 'TASKNAME', 'PROCESSNAME', 'PROCESS', 'AUTORETRYMAXATTEMPTS', 'ROOTSTREAM', 'ROOTSTREAMID', 'STREAMEXECUTIONNUMBER', 'ISLATESTSTREAM', 'STREAM', 'STREAMID', 'PROCESSINSTANCE', 'PROCESS', 'STREAM', 'PROCESSINGSTATUS', 'CREATEDATE', 'READYDATE', 'QUEUEDATE', 'SUBMITDATE', 'STARTDATE', 'ENDDATE', 'EXITCODE', 'JOBSITE', 'JOBID', 'EXECUTIONNUMBER', 'AUTORETRYNUMBER', 'ISLATEST', 'PROCESSINSTANCE', 'EXECUTIONHOST', 'WORKINGDIR', 'LOGFILE', 'CPUSECONDSUSED', 'MEMORYUSED', 'SWAPUSED']

#####################################################################################


## Part Ib: Summary report functions

def dumpStats(xkey, stats):
   print '\nWorkflow summary for: ',xkey
   keys = stats.keys()
   print 'keys = ',keys
   for key in keys:
      print key,stats[key]
      pass
   return


def printStats(title,stats):
   # Nicely formatted tables using 'tabulate'
   #print 'title=',title
   #print 'stats=',stats
   print tabulate.tabulate(stats,headers=title,tablefmt="psql")
   return




## PART II:  Collect Pipeline data and put it into useful organization
#######################################################################

print '\tDC2 Run 1.2p summary'
print '\t===================='
print '  ',datetime.datetime.now(),' Pacific'
print '  (Note: the "ALL" column is currently unreliable)'


## Statistical tallies templates
stats = {"WAITING":0,"PENDING":0,"RUNNING":0,"SUCCESS":0,"FAILED":0,"TERMINATED":0,"ALL":0}
statsL = ["WAITING","PENDING","RUNNING","SUCCESS","FAILED","TERMINATED","ALL"]
allStats = {}             # tallies per task per processStep
globalStats1 = {}         # tallies per processStep over all tasks
for step in taskSteps:
   globalStats1[step]=dict(stats)
   pass
globalStats2 = dict(stats)# tallies over all tasks and processSteps
retried = 0

## Loop over all tasks in DC2-phoSim

for task in taskList:
   if debug:print 'task = ',task

# Build tally container for this task
   allStats[task] = {}
   for step in taskSteps:
      allStats[task][step]=dict(stats)
      pass


## Query Pipeline database
   (colnames,rows) = query(task)
   if debug:
      print 'len(rows) = ',len(rows)
      print '\ncolnames = ',colnames
      print 'rows[0] = ',rows[0]
      pass

## Loop over each processStep instance
   for row in rows:
      if row[colnames.index('ISLATEST')] != 1:
         retried += 1
         continue   ## This is a previoius instance that was rolled back
      
      pname = row[colnames.index('PROCESSNAME')]
      pstate = row[colnames.index('PROCESSINGSTATUS')]
      #if debug:print task,' : ', pname,' : ',pstate
      #  Fill in tallies
      #print 'allStats[',task,'][',pname,'] = ',allStats[task][pname]

      state = pstate
      if pstate in failed:state='FAILED'
      if pstate in pending:state='PENDING'

## Collect and tally the workflow state of every (current) process instance
      if state in stats.keys():
         allStats[task][pname][state] += 1
         globalStats1[pname][state] += 1
         globalStats2[state] += 1
      else:
         print '$ERROR: Unknown process state, ',state,', for processname ',pname
         sys.exit(1)
      pass
   pass


## Part III - Print summary report
############################################################################
      
print "\n=======Task and (batch) Process-step Rollup=============================="
#dumpStats('Task & (batch) processStep roll-up',globalStats2)
header=statsL
table = []
row = []
for state in statsL:
   row.append(globalStats2[state])
   pass
table.append(row)
printStats(header,table)

print "\n=======Task Rollup==============================================="
#dumpStats('Task roll-up',globalStats1)

header=['ProcessStep']+statsL
table = []
for step in taskSteps:
   row = [step]
   for state in statsL:
      row.append(globalStats1[step][state])
      pass
   table.append(row)
   pass
printStats(header,table)

print "\n======================================================\n"

print 'Total rolled back process steps = ',retried  

print "\n=======Detailed Task Summary======================================"
#for task in taskList:
#   dumpStats(task,allStats[task])
#   pass
#####
## Reorganize the data
## To use the 'tabulate' package, one needs a list of lists

header=['Task','ProcessStep']+statsL
for task in taskList:
   table = []
   for step in taskSteps:
      row = [task,step]
      for state in statsL:
         if debug: print 'task: ',task,', step: ',step,', state: ',state,', allStats[task][step][state] = ',allStats[task][step][state]
         row.append(allStats[task][step][state])
         pass
      if debug: print 'row = ',row
      table.append(row)
      pass
   printStats(header,table)
   pass

endTime = datetime.datetime.now()
elapsed = endTime-startTime
print '\nElapsed time for workflow database queries = ',elapsed.total_seconds(),' sec'
sys.exit()
