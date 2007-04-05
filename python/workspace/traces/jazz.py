import ConfigParser, os
from mx.DateTime import *
from mx.DateTime import Parser
from workspace.traces.files import TraceFile, TraceEntryV3

GENERAL_SEC = "general"

IGNOREQUEUE_OPT = "ignore-queue"
START_OPT = "start"
END_OPT = "end"

class JazzConf(object):
    
    def __init__(self, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        self.ignorequeue = []
        
        if config.has_option(GENERAL_SEC, IGNOREQUEUE_OPT):
            queues = config.get(GENERAL_SEC, IGNOREQUEUE_OPT)
            queues = [v.strip() for v in queues.split(",")]
            self.ignorequeue=queues
            
        self.start = self.end = None
        if config.has_option(GENERAL_SEC, START_OPT):
            start = config.get(GENERAL_SEC, START_OPT)
            self.start = Parser.DateTimeFromString(start, ["us"])
        
        if config.has_option(GENERAL_SEC, END_OPT):
            end = config.get(GENERAL_SEC, END_OPT)
            self.end = Parser.DateTimeFromString(end, ["us"])


class RawLogFileEntry(object):

    def __init__(self, line):
        fields = line.strip().split(";")
        
        self.datetime =  Parser.DateTimeFromString(fields[0], ["us"])
        self.record_type = fields[1]
        self.id_string = fields[2].split(".")[0]
        self.message_text = fields[3]
        
        if self.message_text != "":
            self.params = dict([v.split("=",1) for v in self.message_text.split(" ")])
        else:
            self.params = {}

        
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
        
        
JAZZ_BATCH = 0
JAZZ_AR = 1       
JAZZ_INCOMPLETE = 2       
JAZZ_NOQUEUE = 3
JAZZ_NOSTART = 4
JAZZ_NOEND = 5

JAZZ_END_NORMAL = 0
JAZZ_END_KILLED = 1
JAZZ_END_DELETED = 2
JAZZ_END_ABORT = 3
        
class JazzRequest(object):
    
    def __init__(self):
        self.reqTime = None
        self.reqDuration = None
        self.startTime = None
        self.endTime = None
        self.numnodes = None
        self.type = None
        self.incomplete = None
        self.endReason = None
    
    def printFields(self):
        print "Type: %s" % self.type
        print "Requested: %s" % self.reqTime
        print "Resources: (ncpus:%s, walltime:%s)" % (self.numnodes, self.reqDuration)
        print "Ran from %s to %s (walltime: %s)" % (self.startTime, self.endTime, self.getRealDuration())
        print "End reason: %s" % self.endReason
        print
        
    def getRealDuration(self):
        return self.endTime-self.startTime
        
class LogFile(object):
    
    def endRequest(self, id_string, time, endReason):
        if not self.requests.has_key(id_string):
            # This should not happen. Print an error message.
            pass
        else:
            if self.requests[id_string].reqTime != None and self.requests[id_string].startTime==None:
                # This is a queued request that never started running.
                # We can ignore it.
                pass
            else:
                self.requests[id_string].endTime = time                
                self.requests[id_string].endReason = endReason
    
    def __init__(self, raw, c):
        self.requests = {}
        self.sortedReq = []
        self.c = c
        ignore = []
        

        start = c.start
        end = c.end
        
        for e in raw.entries:
            if e.record_type == "A":
                # From PBS Pro docs:
                # Job was aborted by the server.
                self.endRequest(e.id_string, e.datetime, JAZZ_END_ABORT)                    
            elif e.record_type == "B":
                # From PBS Pro docs:
                # owner=ownername --- Name of party who submitted the
                # resources reservation request.
                # name=reservation_name --- If submitter supplied a name string for the reservation.
                # account=account_string --- If submitter supplied a to be recorded in accounting.
                # queue=queue_name --- The name of the instantiated reservation
                #                      queue if this is a general resources reservation.
                #                      If the resources reservation is for a
                #                      reservation job, this is the name of the
                #                      queue to which the reservation-job belongs.
                # ctime=creation_time --- Time at which the resources reservation
                #                         got created, seconds since the epoch.
                # start=period_start --- Time at which the reservation period is to
                #                        start, in seconds since the epoch.
                # end=period_end --- Time at which the reservation period is to
                #                    end, seconds since the epoch.
                # duration=reservation_duration --- The duration specified or computed for the
                #                                   resources reservation, in seconds.
                # nodes=reserved_nodes --- If nodes with specified properties are
                #                          required, this string is the allocated set.
                # authorized_users=users_list --- The list of acl_users on the queue that is
                #                                 instantiated to service the reservation.
                # authorized_groups=groups_list --- If specified, the list of acl_groups on the
                #                                   queue that is instantiated to service the reservation.
                # authorized_hosts=hosts_list --- If specified, the list of acl_hosts on the
                #                                 queue that is instantiated to service the reservation.
                # resource_list=resources_list --- List of resources requested by the reservation.
                #                                  Resources are listed individually as,
                #                                  for example: resource_list.ncpus=16
                #                                  resource_list.mem=1048676.                
                if not self.requests.has_key(e.id_string):
                    # This should not happen. Print an error message.
                    pass
                else:
                    self.requests[e.id_string].startTime = e.datetime
                    self.requests[e.id_string].reqDuration = TimeFrom(e.params["Resource_List.walltime"])
                    self.requests[e.id_string].numnodes = int(e.params["Resource_List.ncpus"])
            elif e.record_type == "D":
                # From PBS Pro docs:
                # Job was deleted by request. The message_text will contain
                # requestor=user@host to identify who deleted the job.
                self.endRequest(e.id_string, e.datetime, JAZZ_END_DELETED)
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
                    # Incompletely specified. Started before start of trace.
                    r = JazzRequest()
                    r.type = JAZZ_INCOMPLETE
                    r.reqTime = None
                    self.requests[e.id_string] = r
                    self.requests[e.id_string].numnodes = int(e.params["Resource_List.ncpus"])
                    self.sortedReq.append(e.id_string)
                if (e.params["queue"] in c.ignorequeue or e.params["queue"][0]=='R') and not e.id_string in ignore:
                    ignore.append(e.id_string)
                self.endRequest(e.id_string, e.datetime, JAZZ_END_NORMAL)                    
            elif e.record_type == "k":
                # From PBS Pro docs:
                # Scheduler or server requested removal of the reservation. The
                # message_text field contains: requestor=user@host
                # to identify who deleted the resources reservation.
                self.endRequest(e.id_string, e.datetime, JAZZ_END_KILLED)                    
            elif e.record_type == "K":
                # From PBS Pro docs:
                # Resources reservation terminated by ordinary client - e.g. an
                # owner issuing a pbs_rdel command. The message_text
                # field contains: requestor=user@host to identify who
                # deleted the resources reservation.
                self.endRequest(e.id_string, e.datetime, JAZZ_END_KILLED)                    
            elif e.record_type == "Q":
                # From PBS Pro docs:
                # Job entered a queue. The message_text contains queue=name
                # identifying the queue into which the job was placed. There will
                # be a new Q record each time the job is routed or moved to a new
                # (or the same) queue.
                if not self.requests.has_key(e.id_string):
                    r = JazzRequest()
                    r.type = JAZZ_BATCH
                    r.reqTime = e.datetime
                    self.requests[e.id_string] = r
                    self.sortedReq.append(e.id_string)
                    if e.params["queue"] in c.ignorequeue:
                        ignore.append(e.id_string)
                else:
                    # Just moving to a different queue. Not of interest to us.
                    pass
            elif e.record_type == "R":
                # From PBS Pro docs:
                # Job was rerun.
                if not e.id_string in ignore:
                    ignore.append(e.id_string)
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
                if e.params["queue"][0]!='R':
                    if not self.requests.has_key(e.id_string):
                        # Incompletely specified. Queued before start of trace.
                        r = JazzRequest()
                        r.type = JAZZ_INCOMPLETE
                        r.incomplete = JAZZ_NOQUEUE
                        r.reqTime = None
                        self.requests[e.id_string] = r
                        self.sortedReq.append(e.id_string)
                    self.requests[e.id_string].startTime = e.datetime
                    self.requests[e.id_string].numnodes = int(e.params["Resource_List.ncpus"])
                    if e.params.has_key("Resource_List.walltime"):
                        self.requests[e.id_string].reqDuration = TimeFrom(e.params["Resource_List.walltime"])
                    else:
                        self.requests[e.id_string].reqDuration = TimeFrom("15:00:00.00")
                if e.params["queue"] in c.ignorequeue and not e.id_string in ignore:
                    ignore.append(e.id_string)
            elif e.record_type == "U":
                # From PBS Pro docs:
                # Created unconfirmed resources reservation on Server. The
                # message_text field contains requestor=user@host to
                # identify who requested the resources reservation.
                if not self.requests.has_key(e.id_string):
                    r = JazzRequest()
                    r.type = JAZZ_AR
                    r.reqTime = e.datetime
                    self.requests[e.id_string] = r
                    self.sortedReq.append(e.id_string)
                else:
                    # This should not happen. Print an error message.
                    pass
            elif e.record_type == "Y":
                # From PBS Pro docs:
                # Resources reservation confirmed by the Scheduler. The
                # message_text field contains the same item (items) as in a U
                # record type.
                if not self.requests.has_key(e.id_string):
                    # This should not happen. Print error.
                    pass
                
        # Remove rerun jobs
        for j in ignore:
            del self.requests[j]
            self.sortedReq.remove(j)
            
        # Find incompletely specified jobs
        incomplete = []
        for r in self.sortedReq:
            if self.requests[r].startTime == None and self.requests[r].endTime == None:
                incomplete.append(r)
            elif self.requests[r].startTime == None:
                if start != None:
                    self.requests[r].startTime = start
                    self.requests[r].type = JAZZ_INCOMPLETE
                    self.requests[r].incomplete = JAZZ_NOSTART
                else:
                    incomplete.append(r)
            elif self.requests[r].endTime == None:
                if end != None:
                    self.requests[r].endTime = end
                    self.requests[r].type = JAZZ_INCOMPLETE
                    self.requests[r].incomplete = JAZZ_NOEND
                else:
                    incomplete.append(r)
                
        
        # Remove incompletely specified jobs
        for i in incomplete:
            del self.requests[i]
            self.sortedReq.remove(i)
        
        
    def printRequests(self):
        for r in self.sortedReq:
            print r
            self.requests[r].printFields()
        
    def toTrace(self):
        tEntries = []
        startTime = self.c.start
        for r in self.sortedReq:
            numNodes = self.requests[r].numnodes
            if self.requests[r].reqDuration == None:
                duration = None
            else:
                duration = self.requests[r].reqDuration.seconds
            fields = {}
            if self.requests[r].type == JAZZ_INCOMPLETE:
                fields["time"] = `int(startTime.ticks())`
            else:
                fields["time"] = `int(self.requests[r].reqTime.ticks())`                
            fields["uri"] = "NONE"
            fields["size"] = "0"
            fields["numNodes"] = str(numNodes) 
            fields["mode"] = "RW"
            if self.requests[r].type == JAZZ_BATCH:
                fields["deadline"] = "NULL"
                fields["tag"]="BATCH"
            elif self.requests[r].type == JAZZ_AR:
                fields["deadline"] = `int(self.requests[r].startTime)`
                fields["tag"]="AR"
            elif self.requests[r].type == JAZZ_INCOMPLETE:
                fields["deadline"] = `int(self.requests[r].startTime)`
                if self.requests[r].incomplete == JAZZ_NOSTART:
                    fields["tag"]="INCOMPLETE_NOSTART"            
                elif self.requests[r].incomplete == JAZZ_NOEND:
                    fields["tag"]="INCOMPLETE_NOEND"
                elif self.requests[r].incomplete == JAZZ_NOQUEUE:
                    fields["tag"]="INCOMPLETE_NOQUEUE"
                else:            
                    fields["tag"]="INCOMPLETE"
            fields["realduration"] = `int(self.requests[r].getRealDuration().seconds)`
            fields["realstart"] = `int(self.requests[r].startTime.ticks())`
            if self.requests[r].type == JAZZ_INCOMPLETE:
                fields["duration"] = fields["realduration"]
            else:
                fields["duration"] = `int(duration)`
            fields["cpu"] = "100"
            fields["memory"] = "100"
            tEntries.append(TraceEntryV3(fields))
        tEntries.sort(key=lambda x: int(x.fields["time"]))
        return TraceFile(tEntries)        
        