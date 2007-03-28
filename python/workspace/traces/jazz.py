import ConfigParser, os
from mx.DateTime import *
from mx.DateTime import Parser

class JazzConf(object):
    
    def __init__(self, filename):
        pass

class RawLogFileEntry(object):

    def __init__(self, line):
        fields = line.strip().split(";")
        
        self.datetime =  Parser.DateTimeFromString(fields[0], ["us"])
        self.record_type = fields[1]
        self.id_string = fields[2].split(".")[0]
        self.message_text = fields[3]
        
        self.params = dict([v.split("=") for v in self.message_text.split(" ")])

        
    def printFields(self):
        print "%s %s %s %s" % (self.datetime, self.record_type, self.id_string, self.params)

class RawLogFile(object):
    
    def __init__(self, filename):
        self.entries = []

        file = open (filename, "r")
        for line in file:
            e = RawLogFileEntry(line)
            self.entries.append(e)
            
    def printEntries(self):
        for e in self.entries:
            e.printFields()
        
        
class JazzRequest(object):
    
    def __init__(self):
        self.reqTime = None
        self.reqDuration = None
        self.startTime = None
        self.endTime = None
        self.numnodes = None
    
    def printFields(self):
        print "Requested: %s" % self.reqTime
        print "Resources: (ncpus:%s, walltime:%s)" % (self.numnodes, self.reqDuration)
        print "Ran from %s to %s" % (self.startTime, self.endTime)
        print
        
class LogFile(object):
    
    def __init__(self, raw, c):
        self.requests = {}
        self.sortedReq = []
        
        for e in raw.entries:
            if e.record_type == "A":
                pass
            elif e.record_type == "B":
                pass
            elif e.record_type == "D":
                pass
            elif e.record_type == "E":
                # From PBS Pro docs:
                # Job ended (terminated execution). The message_text field
                # contains detailed information about the job. Possible attributes
                # include:
                # user=username --- The user name under which the job executed.
                # group=groupname --- The group name under which the job executed.
                # account=account_string --- If job has an "account name" string.
                # jobname=job_name --- The name of the job.
                # queue=queue_name --- The name of the queue in which the job executed.
                # resvname=reservation_name --- The name of the resource reservation, if applicable.
                # resvID=reservation_ID_string --- The ID of the resource reservation, if applicable.
                # resvjobID=reservation_ID_string --- The ID of the resource reservation, if applicable.
                # ctime=time --- Time in seconds when job was created (first submitted).
                # qtime=time --- Time in seconds when job was queued into current queue.
                # etime=time --- Time in seconds when job became eligible to run.
                # start=time --- Time in seconds when job execution started.
                # exec_host=host --- Name of host on which the job is being executed.
                # Resource_List.resource=limit --- List of the specified resource limits.
                # session=sessionID --- Session number of job.
                # alt_id=id --- Optional alternate job identifier. 
                # end=time --- Time in seconds when job ended execution.
                # Exit_status=value --- The exit status of the job. If the value is less 
                #                       than 10000 (decimal) it is the exit value of
                #                       the top level process of the job, typically the
                #                       shell. If the value is greater than 10000, the
                #                       top process exited on a signal whose number
                #                       is given by subtracting 10000 from the exit
                #                       value.
                # Resources_used.resource=usage_amount Amount of specified resource used over the duration of the job.
                if not self.requests.has_key(e.id_string):
                    # This should not happen. Print an error message.
                    pass
                else:
                    self.requests[e.id_string].endTime = e.datetime
            elif e.record_type == "k":
                pass
            elif e.record_type == "K":
                pass
            elif e.record_type == "Q":
                # From PBS Pro docs:
                # Job entered a queue. The message_text contains queue=name
                # identifying the queue into which the job was placed. There will
                # be a new Q record each time the job is routed or moved to a new
                # (or the same) queue.
                if not self.requests.has_key(e.id_string):
                    r = JazzRequest()
                    r.reqTime = e.datetime
                    self.requests[e.id_string] = r
                    self.sortedReq.append(e.id_string)
                else:
                    # Just moving to a different queue. Not of interest to us.
                    pass
            elif e.record_type == "R":
                pass
            elif e.record_type == "S":
                # From PBS Pro docs:
                # Job execution started. The message_text field contains:
                # user=username --- The user name under which the job will execute.
                # group=groupname --- The group name under which the job will execute.
                # jobname=job_name --- The name of the job.
                # queue=queue_name --- The name of the queue in which the job resides.
                # ctime=time --- Time in seconds when job was created (first submitted).
                # qtime=time --- Time in seconds when job was queued into current queue.
                # etime=time --- Time in seconds when job became eligible to run; no holds, etc.
                # start=time --- Time in seconds when job execution started.
                # exec_host=host --- Name of host on which the job is being executed.
                # Resource_List.resource=limit --- List of the specified resource limits.
                # session=sessionID --- Session number of job.
                if not self.requests.has_key(e.id_string):
                    # This should not happen. Print an error message.
                    pass
                else:
                    self.requests[e.id_string].startTime = e.datetime
                    self.requests[e.id_string].reqDuration = TimeFrom(e.params["Resource_List.walltime"])
                    self.requests[e.id_string].numnodes = int(e.params["Resource_List.ncpus"])
            elif e.record_type == "U":
                pass
            elif e.record_type == "Y":
                pass
        
        
    def printRequests(self):
        for r in self.sortedReq:
            self.requests[r].printFields()
        
        
        