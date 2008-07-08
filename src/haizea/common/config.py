# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

import ConfigParser
from mx.DateTime import ISO
from mx.DateTime import TimeDelta
import haizea.common.constants as constants
import haizea.common.stats as stats
import os.path
from haizea.common.utils import genDataDirName

class Config(object):
    def __init__(self, config):
        self.config = config
        
    @classmethod
    def fromFile(cls, configfile):
        file = open (configfile, "r")
        c = ConfigParser.ConfigParser()
        c.readfp(file)
        return cls(c)
       
    def createDiscreteDistributionFromSection(self, section):
        distType = self.config.get(section, constants.DISTRIBUTION_OPT)
        probs = None
        if self.config.has_option(section, constants.MIN_OPT) and self.config.has_option(section, constants.MAX_OPT):
            min = self.config.getint(section, constants.MIN_OPT)
            max = self.config.getint(section, constants.MAX_OPT)
            values = range(min, max+1)
        elif self.config.has_option(section, constants.ITEMS_OPT):
            pass
        elif self.config.has_option(section, constants.ITEMSPROBS_OPT):
            pass
#        elif self.config.has_option(section, constants.ITEMSFILE_OPT):
#            filename = config.get(section, constants.ITEMSFILE_OPT)
#            file = open (filename, "r")
#            values = []
#            for line in file:
#                value = line.strip().split(";")[0]
#                values.append(value)
#        elif self.config.has_option(section, constants.ITEMSPROBSFILE_OPT):
#            itemsprobsOpt = self.config.get(section, constants.ITEMSPROBSFILE_OPT).split(",")
#            itemsFile = open(itemsprobsOpt[0], "r")
#            probsField = int(itemsprobsOpt[1])
#            values = []
#            probs = []
#            for line in itemsFile:
#                fields = line.split(";")
#                itemname = fields[0]
#                itemprob = float(fields[probsField])/100
#                values.append(itemname)
#                probs.append(itemprob)
        dist = None
        if distType == constants.DIST_UNIFORM:
            dist = stats.DiscreteUniformDistribution(values)
        elif distType == constants.DIST_EXPLICIT:
            if probs == None:
                raise Exception, "No probabilities specified"
            dist = stats.DiscreteDistribution(values, probs) 
            
        return dist
        
    def createContinuousDistributionFromSection(self, section):
        distType = self.config.get(section, constants.DISTRIBUTION_OPT)
        min = self.config.getfloat(section, constants.MIN_OPT)
        max = self.config.get(section, constants.MAX_OPT)
        if max == "unbounded":
            max = float("inf")
        if distType == "uniform":
            dist = stats.ContinuousUniformDistribution(min, max)
        elif distType == "normal":
            mu = self.config.getfloat(section, constants.MEAN_OPT)
            sigma = self.config.getfloat(section, constants.STDEV_OPT)
            dist = stats.ContinuousNormalDistribution(min, max, mu, sigma)
        elif distType == "pareto":
            pass 
        
        return dist
        

        
class RMConfig(Config):
    def __init__(self, config):
        Config.__init__(self, config)
        
    #
    # GENERAL OPTIONS
    #

    def getLogLevel(self):
        return self.config.get(constants.GENERAL_SEC, constants.LOGLEVEL_OPT)
    
    def getProfile(self):
        return self.config.get(constants.GENERAL_SEC, constants.PROFILE_OPT)

    def getMode(self):
        return self.config.get(constants.GENERAL_SEC, constants.MODE_OPT)

    def get_lease_deployment_type(self):
        if not self.config.has_option(constants.GENERAL_SEC, constants.LEASE_DEPLOYMENT_OPT):
            return constants.DEPLOYMENT_UNMANAGED
        else:
            return self.config.get(constants.GENERAL_SEC, constants.LEASE_DEPLOYMENT_OPT)

    def getDataDir(self):
        return self.config.get(constants.GENERAL_SEC, constants.DATADIR_OPT)

    #
    # SIMULATION OPTIONS
    #
        
    def getInitialTime(self):
        timeopt = self.config.get(constants.SIMULATION_SEC, constants.STARTTIME_OPT)
        return ISO.ParseDateTime(timeopt)
    
    def getNumPhysicalNodes(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.NODES_OPT)
    
    def getResourcesPerPhysNode(self):
        return self.config.get(constants.SIMULATION_SEC, constants.RESOURCES_OPT).split(";")
    
    def getBandwidth(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.BANDWIDTH_OPT)

    def getSuspendResumeRate(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.SUSPENDRATE_OPT)
    
    def stopWhen(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.STOPWHEN_OPT):
            return None
        else:
            return self.config.get(constants.SIMULATION_SEC, constants.STOPWHEN_OPT)

    def getForceTransferTime(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.FORCETRANSFERT_OPT):
            return None
        else:
            return TimeDelta(seconds=self.config.getint(constants.SIMULATION_SEC, constants.FORCETRANSFERT_OPT))

    def getRuntimeOverhead(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.RUNOVERHEAD_OPT):
            return None
        else:
            return self.config.getint(constants.SIMULATION_SEC, constants.RUNOVERHEAD_OPT)

    def getBootOverhead(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.BOOTOVERHEAD_OPT):
            time = 0
        else:
            time = self.config.getint(constants.SIMULATION_SEC, constants.BOOTOVERHEAD_OPT)
        return TimeDelta(seconds=time)

    def overheadOnlyBestEffort(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.RUNOVERHEADBE_OPT):
            return False
        else:
            return self.config.getboolean(constants.SIMULATION_SEC, constants.RUNOVERHEADBE_OPT)

    def getStatusMessageInterval(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.STATUS_INTERVAL_OPT):
            return None
        else:
            return self.config.getint(constants.SIMULATION_SEC, constants.STATUS_INTERVAL_OPT)

    #
    # OPENNEBULA OPTIONS
    #
    def getONEDB(self):
        return self.config.get(constants.OPENNEBULA_SEC, constants.DB_OPT)

    def getONESuspendResumeRate(self):
        return self.config.getint(constants.OPENNEBULA_SEC, constants.ESTIMATESUSPENDRATE_OPT)

    #
    # SCHEDULING OPTIONS
    #

    def getSuspensionType(self):
        return self.config.get(constants.SCHEDULING_SEC, constants.SUSPENSION_OPT)

    def isMigrationAllowed(self):
        return self.config.getboolean(constants.SCHEDULING_SEC, constants.MIGRATION_OPT)

    def getMustMigrate(self):
        return self.config.get(constants.SCHEDULING_SEC, constants.MIGRATE_OPT)

    def getMaxReservations(self):
        if self.getBackfillingType() == constants.BACKFILLING_OFF:
            return 0
        elif self.getBackfillingType() == constants.BACKFILLING_AGGRESSIVE:
            return 1
        elif self.getBackfillingType() == constants.BACKFILLING_CONSERVATIVE:
            return 1000000
        elif self.getBackfillingType() == constants.BACKFILLING_INTERMEDIATE:
            r = self.config.getint(constants.SCHEDULING_SEC, constants.RESERVATIONS_OPT)
            return r

    def getSuspendThreshold(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLD_OPT):
            return None
        else:
            return TimeDelta(seconds=self.config.getint(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLD_OPT))

    def getSuspendThresholdFactor(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLDFACTOR_OPT):
            return 0
        else:
            return self.config.getfloat(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLDFACTOR_OPT)

    def isBackfilling(self):
        if self.getBackfillingType() == constants.BACKFILLING_OFF:
            return False
        else:
            return True
        
    def getBackfillingType(self):
        return self.config.get(constants.SCHEDULING_SEC, constants.BACKFILLING_OPT)


    #
    # DEPLOYMENT (IMAGETRANSFER) OPTIONS
    #

    def get_transfer_mechanism(self):
        return self.config.get(constants.DEPLOY_IMAGETRANSFER_SEC, constants.TRANSFER_MECHANISM_OPT)

    def getReuseAlg(self):
        if not self.config.has_option(constants.DEPLOY_IMAGETRANSFER_SEC, constants.REUSE_OPT):
            return constants.REUSE_NONE
        else:
            return self.config.get(constants.DEPLOY_IMAGETRANSFER_SEC, constants.REUSE_OPT)
        
    def getMaxCacheSize(self):
        if not self.config.has_option(constants.DEPLOY_IMAGETRANSFER_SEC, constants.CACHESIZE_OPT):
            return constants.CACHESIZE_UNLIMITED
        else:
            return self.config.getint(constants.DEPLOY_IMAGETRANSFER_SEC, constants.CACHESIZE_OPT)        
        
    def isAvoidingRedundantTransfers(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.AVOIDREDUNDANT_OPT):
            return False
        else:
            return self.config.getboolean(constants.DEPLOY_IMAGETRANSFER_SEC, constants.AVOIDREDUNDANT_OPT)

    def getNodeSelectionPolicy(self):
        if not self.config.has_option(constants.DEPLOY_IMAGETRANSFER_SEC, constants.NODESELECTION_OPT):
            return constants.NODESELECTION_AVOIDPREEMPT
        else:
            return self.config.get(constants.DEPLOY_IMAGETRANSFER_SEC, constants.NODESELECTION_OPT)


    #
    # TRACEFILE OPTIONS
    #
    def getTracefile(self):
        return self.config.get(constants.TRACEFILE_SEC, constants.TRACEFILE_OPT)

    def getInjectfile(self):
        if not self.config.has_option(constants.TRACEFILE_SEC, constants.INJFILE_OPT):
            return None
        else:
            injfile = self.config.get(constants.TRACEFILE_SEC, constants.INJFILE_OPT)
            if injfile == "None":
                return None
            else:
                return injfile

    def getImagefile(self):
        if not self.config.has_option(constants.TRACEFILE_SEC, constants.IMGFILE_OPT):
            return None
        else:
            imgfile = self.config.get(constants.TRACEFILE_SEC, constants.IMGFILE_OPT)
            if imgfile == "None":
                return None
            else:
                return imgfile

class RMMultiConfig(Config):
    def __init__(self, config):
        Config.__init__(self, config)
        
    def getProfiles(self):
        sections = set([s.split(":")[0] for s in self.config.sections()])
        # Remove multi and common sections
        sections.difference_update([constants.COMMON_SEC, constants.MULTI_SEC])
        return list(sections)
    
    def getProfilesSubset(self, sec):
        profiles = self.config.get(sec, constants.PROFILES_OPT)
        if profiles == "ALL":
            profiles = self.getProfiles()
        else:
            profiles = profiles.split()
        return profiles

    def getTracesSubset(self, sec):
        traces = self.config.get(sec, constants.TRACES_OPT)
        if traces == "ALL":
            traces = [os.path.basename(t) for t in self.getTracefiles()]
        else:
            traces = traces.split()
            
        return traces

    def getInjSubset(self, sec):
        injs = self.config.get(sec, constants.INJS_OPT)
        if injs == "ALL":
            injs = [os.path.basename(t) for t in self.getInjectfiles() if t!=None]
            injs.append(None)
        elif injs == "NONE":
            injs = [None]
        else:
            injs = injs.split()
        return injs

    def getTracefiles(self):
        dir = self.config.get(constants.MULTI_SEC, constants.TRACEDIR_OPT)
        traces = self.config.get(constants.MULTI_SEC, constants.TRACEFILES_OPT).split()
        return [dir + "/" + t for t in traces]

    def getInjectfiles(self):
        dir = self.config.get(constants.MULTI_SEC, constants.INJDIR_OPT)
        inj = self.config.get(constants.MULTI_SEC, constants.INJFILES_OPT).split()
        inj = [dir + "/" + i for i in inj]
        inj.append(None)
        return inj
    
    def getConfigs(self):
        profiles = self.getProfiles()
        tracefiles = self.getTracefiles()
        injectfiles = self.getInjectfiles()

        configs = []
        for profile in profiles:
            for tracefile in tracefiles:
                for injectfile in injectfiles:
                    profileconfig = ConfigParser.ConfigParser()
                    commonsections = [s for s in self.config.sections() if s.startswith("common:")]
                    profilesections = [s for s in self.config.sections() if s.startswith(profile +":")]
                    sections = commonsections + profilesections
                    for s in sections:
                        s_noprefix = s.split(":")[1]
                        items = self.config.items(s)
                        if not profileconfig.has_section(s_noprefix):
                            profileconfig.add_section(s_noprefix)
                        for item in items:
                            profileconfig.set(s_noprefix, item[0], item[1])
                            
                    # The tracefile section may have not been created
                    if not profileconfig.has_section(constants.TRACEFILE_SEC):
                        profileconfig.add_section(constants.TRACEFILE_SEC)

                    # Add tracefile option
                    profileconfig.set(constants.TRACEFILE_SEC, constants.TRACEFILE_OPT, tracefile)
                    
                    # Add injected file option
                    if injectfile == None:
                        inj = "None"
                    else:
                        inj = injectfile
                    profileconfig.set(constants.TRACEFILE_SEC, constants.INJFILE_OPT, inj)

                    # Add datadir option
                    datadirname = genDataDirName(profile, tracefile, injectfile)
                    basedatadir = self.config.get(constants.MULTI_SEC, constants.BASEDATADIR_OPT)
                    datadir = basedatadir + "/" + datadirname
                    profileconfig.set(constants.GENERAL_SEC, constants.DATADIR_OPT, datadir)
                    
                    # Set profile option (only used internally)
                    profileconfig.set(constants.GENERAL_SEC, constants.PROFILE_OPT, profile)
                    
                    c = RMConfig(profileconfig)
                    configs.append(c)
        
        return configs

            
    def getConfigsToRun(self):
        configs = self.getConfigs()
        
        # TODO: Come up with a new way to filter what gets run or not
        #profiles = self.getProfilesSubset(constants.RUN_SEC)
        #traces = self.getTracesSubset(constants.RUN_SEC)
        #injs = self.getInjSubset(constants.RUN_SEC)
        
#        confs = []
#        for c in configs:
#            p = c.getProfile()
#            t = os.path.basename(c.getTracefile())
#            i = c.getInjectfile()
#            if i != None: 
#                i = os.path.basename(i)
#
#            if p in profiles and t in traces and i in injs:
#                confs.append(c)
#
#        return confs
        return configs

    

        
class TraceConfig(Config):
    def __init__(self, c):
        Config.__init__(self, c)
        self.numnodesdist = self.createDiscreteDistributionFromSection(constants.NUMNODES_SEC)
        self.deadlinedist = self.createDiscreteDistributionFromSection(constants.DEADLINE_SEC)
        self.durationdist = self.createDiscreteDistributionFromSection(constants.DURATION_SEC)
        self.imagesdist = self.createDiscreteDistributionFromSection(constants.IMAGES_SEC)
        if self.isGenerateBasedOnWorkload():
            # Find interval between requests
            tracedur = self.getTraceDuration()
            percent = self.getPercent()
            nodes = self.getNumNodes()
            accumduration = tracedur * nodes * percent
            numreqs = accumduration / (self.numnodesdist.getAvg() * self.durationdist.getAvg())
            intervalavg = int(tracedur / numreqs)
            min = intervalavg - 3600 # Make this configurable
            max = intervalavg + 3600 # Make this configurable
            values = range(min, max+1)
            self.intervaldist = stats.DiscreteUniformDistribution(values)
        else:
            self.intervaldist = self.createDiscreteDistributionFromSection(constants.INTERVAL_SEC)
        
    def getTraceDuration(self):
        return self.config.getint(constants.GENERAL_SEC, constants.DURATION_OPT)
        
    def getPercent(self):
        percent = self.config.getint(constants.WORKLOAD_SEC, constants.PERCENT_OPT)
        percent = percent / 100.0
        return percent
    
    def getNumNodes(self):
        return self.config.getint(constants.WORKLOAD_SEC, constants.NUMNODES_OPT)

    def getDuration(self):
        return self.durationdist.get()
    
    def isGenerateBasedOnWorkload(self):
        return self.config.has_section(constants.WORKLOAD_SEC)


    
class ImageConfig(Config):
    def __init__(self, c):
        Config.__init__(self, c)
        self.sizedist = self.createDiscreteDistributionFromSection(constants.SIZE_SEC)
        numimages = self.config.getint(constants.GENERAL_SEC, constants.IMAGES_OPT)
        self.images = ["image_" + str(i+1) for i in range(numimages)]
        
        distribution = self.config.get(constants.GENERAL_SEC, constants.DISTRIBUTION_OPT)
        if distribution == "uniform":
            self.imagedist = stats.DiscreteUniformDistribution(self.images) 
        else:
            probs = []
            explicitprobs = distribution.split()
            for p in explicitprobs:
                numitems, prob = p.split(",")
                itemprob = float(prob)/100
                for i in range(int(numitems)):
                    probs.append(itemprob)
            self.imagedist = stats.DiscreteDistribution(self.images, probs)
            print probs
    
    def getFileLength(self):
        return self.config.getint(constants.GENERAL_SEC, constants.LENGTH_OPT)
        

        
        