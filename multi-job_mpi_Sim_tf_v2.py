#!/usr/bin/env python
from mpi4py import MPI
import sys
import os
from subprocess import call
import time
from os.path import abspath as _abspath, join as _join

#---------------------------------------------
# Main start here
#---------------------------------------------
# Obtain MPI rank
def main():
    start_g =time.time()
    start_g_str = time.asctime(time.localtime(start_g))
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    max_rank=comm.Get_size()
    print("%s | Rank: %s from %s ranks started" % (start_g_str, rank, max_rank))

    # Get a file name with job descriptions
    input_file = sys.argv[1]
    
    #Read file into an array removing new line.
    try:
        file = open(input_file)
        temp = file.read().splitlines()
        file.close()
    except IOError as (errno, strerror):
        print "I/O error({0}): {1}".format(errno, strerror)
        print("Exit from rank %s" % rank)
        return
    
    # Number of commands in the input file
    n_comm = len(temp)
    #print("Number of commands: %s" % n_comm)
    
    # Safeguard here. Just in case things are messed up.
    if n_comm != max_rank:
        print("Rank: %s  Problem with input file! Number of commands not equal to number of ranks! Exiting")
        exit()
    
    #PandaID of the job for the command
    wkdirname = temp[rank].split("!!")[0].strip()
    # Get the corresponding command from input file
    my_command = temp[rank].split("!!")[1]
    #filename for stdout & srderr
    payloadstdout_s = temp[rank].split("!!")[2]
    payloadstderr_s = temp[rank].split("!!")[3]
    
    
    PanDA_jobId = wkdirname[wkdirname.find("_")+1:]    
    #Modify pathes for input files
    #--------------------------------------------

    # Fix 'poor' arguments
    subs_a = my_command.split()
    for i in range(len(subs_a)):
        if i > 0:
            if '(' in subs_a[i] and not subs_a[i][0] == '"':
                subs_a[i] = '"'+subs_a[i]+'"'
            
    my_command = ' '.join(subs_a)
    my_command = my_command.strip()
    my_command = my_command.replace('--DBRelease="all:current"', '') # avoid Frontier reading

    # create working directory for a given rank
    curdir = _abspath (os.curdir)

    # wkdirname = "PandaJob_%s" % my_job_index
    wkdir = _abspath (_join(curdir,wkdirname))
    if not os.path.exists(wkdir):
        os.mkdir(wkdir)
    '''
        print "Directory ", wkdir, " created"
    else:
        print "Directory ", wkdir, " exists"
    '''
    # Go to local working directory (job rank)
    os.chdir(wkdir)
    
    #---------
    # Copy Poolcond files to local working directory
    if not os.path.exists("./poolcond"):
        os.mkdir("./poolcond")
    prep_comm="cp /lustre/atlas/proj-shared/csc108/app_dir/atlas_app/atlas_rel/21.0.15/DBRelease/current/poolcond/*.xml  ./poolcond/."
    #This should be done in the rank's working directory
    # Dirty trick  to solve problems with access to DB
    #true_sql_dir = "/lustre/atlas/proj-shared/csc108/app_dir/atlas_app/atlas_rel/19.2.4/DBRelease/current/sqlite200/"

    true_sql_dir = "/lustre/atlas/proj-shared/csc108/ATLAS_db/21.0.15/DBRelease/current/sqlite200"
    pseudo_sql_dir = "./sqlite200"
    os.symlink(true_sql_dir, pseudo_sql_dir)
    
    #--------
    pcmd = call(prep_comm, shell=True)
    localtime = time.asctime(time.localtime(time.time()))
    print("%s | %s Rank: %s Finished copying XML files " % (localtime,PanDA_jobId, rank))
    #---------

    payloadstdout = open(payloadstdout_s,"w")
    payloadstderr = open(payloadstderr_s,"w")
    
    localtime = time.asctime(time.localtime(time.time()))
    print("%s | %s Rank: %s Starting payload: %s" % (localtime, PanDA_jobId, rank, my_command))
    t0 = os.times()
    exit_code = call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    localtime = time.asctime(time.localtime(time.time()))
    t = map(lambda x, y:x-y, t1, t0)
    t_tot = reduce(lambda x, y:x+y, t[2:3])
    
    report = open("rank_report.txt","w")
    report.write("cpuConsumptionTime: %s\n" % t_tot)
    report.write("exitCode: %s" % exit_code)
    report.close()
    
    # Finished all steps here
    localtime = time.asctime( time.localtime(time.time()) )
    end_all = time.time()
    print("%s | %s Rank: %s Finished. Program run took.  %s min. Exit code: %s cpuConsumptionTime: %s" % (localtime, PanDA_jobId,rank,(end_all-start_g)/60. ,exit_code, t_tot))

    return 0
    # Wait for all ranks to finish
    #comm.Barrier()

if __name__ == "__main__":
    sys.exit(main())
