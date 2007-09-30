def genDataDirName(profile, tracefile, injectedfile):
    tracename=tracefile.split("/")[-1].split(".")[0]
    
    if injectedfile != None:
        injectname=injectedfile.split("/")[-1].split(".")[0]
        dir = profile + "/" + tracename + "+" + injectname + "/"
    else:
        dir = profile + "/" + tracename + "/"
    
    return dir