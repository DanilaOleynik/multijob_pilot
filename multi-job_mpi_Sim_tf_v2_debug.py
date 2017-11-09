#!/usr/bin/env python
from mpi4py import MPI
import sys
import os
from socket import gethostname
from subprocess import call
import time
from os.path import abspath as _abspath, join as _join
from shutil import copyfile

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
    hostname = gethostname()
    scratch_path = '/tmp/scratch/'

    # Cleanup of enviroment
    '''
    try:
        if rank == 0:
            print '----------------------'
            for i in os.environ.keys():
                if '/lustre/' in os.environ[i]:
                    print i
                    path = os.environ[i].split(':')
                    for pp in path:
                        print pp
                    print '----------------------'
    except:
        pass
    '''

    path = os.environ['PATH'].split(':')
    for p in path[:]:
        if p.startswith("/lustre/"):
            path.remove(p)
    ppath = os.environ['PYTHONPATH'].split(':')
    for p in ppath[:]:
        if p.startswith("/lustre/"):
            ppath.remove(p)
    ldpath = os.environ['LD_LIBRARY_PATH'].split(':')
    for p in ldpath[:]:
        if p.startswith("/lustre/"):
            ldpath.remove(p)

    #pbs_o_path = os.environ['PBS_O_PATH'].split(':')
    #for p in pbs_o_path[:]:
    #    if p.startswith("/lustre/"):
    #        pbs_o_path.remove(p)

    os.environ['PATH'] = ':'.join(path)
    os.putenv('PATH', ':'.join(path))
    os.environ['PYTHONPATH'] = ':'.join(ppath)
    os.putenv('PYTHONPATH', ':'.join(ppath))
    os.environ['LD_LIBRARY_PATH'] = ':'.join(ldpath)
    os.putenv('LD_LIBRARY_PATH', ':'.join(ldpath))
    #os.environ['PBS_O_PATH'] = ':'.join(pbs_o_path)
    #os.putenv('PBS_O_PATH', ':'.join(pbs_o_path))

    print("%s | Rank: %s from %s ranks started on node [ %s ]" % (start_g_str, rank, max_rank, hostname))

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

    # Fix 'poor' arguments and change path of input data to RAM disk

    inputfiles = []
    subs_a = my_command.split()
    for i in range(len(subs_a)):
        if i > 0:
            if '(' in subs_a[i] and not subs_a[i][0] == '"':
                subs_a[i] = '"'+subs_a[i]+'"'
            if subs_a[i].startswith("--inputEVNTFile"):
                filename = subs_a[i].split("=")[1]
                inputfiles.append((filename, scratch_path + filename))
                subs_a[i] = subs_a[i].replace(filename, scratch_path + filename)

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
    strace_log_path = "/lustre/atlas/proj-shared/csc108/pilots_workdir1/strace_log_%s.txt " % PanDA_jobId
    env_file = "/lustre/atlas/proj-shared/csc108/pilots_logs/env_%s.txt" % PanDA_jobId
    syspath_file = "/lustre/atlas/proj-shared/csc108/pilots_logs/syspath_%s.txt" % PanDA_jobId
    pathes = "/lustre/atlas/proj-shared/csc108/pilots_logs/pathes_%s.txt" % PanDA_jobId
    strace = 'strace -f -o ' + strace_log_path

    #---------
    # Copy Poolcond files to local working directory

    #io_prep_starttime = time.time()
    #if not os.path.exists("./poolcond"):
    #    os.mkdir("./poolcond")
    #prep_comm="cp /lustre/atlas/proj-shared/csc108/app_dir/atlas_app/atlas_rel/21.0.15/DBRelease/current/poolcond/*.xml  ./poolcond/."
    #This should be done in the rank's working directory
    # Dirty trick  to solve problems with access to DB
    # true_sql_dir = "/lustre/atlas/proj-shared/csc108/app_dir/atlas_app/atlas_rel/19.2.4/DBRelease/current/sqlite200/"
    # true_sql_dir = "/lustre/atlas/proj-shared/csc108/ATLAS_db/21.0.15/DBRelease/current/sqlite200"
    # true_sql_dir = '/lustre/atlas/proj-shared/csc108/ATLAS_DB_s1/21.0.15/DBRelease/current/sqlite200'
    # pseudo_sql_dir = "./sqlite200"
    # os.symlink(true_sql_dir, pseudo_sql_dir)

    # Copy sqlite DB to scratch on working node and linking 'poolcond'



    dst_db_path = 'sqlite200/'
    dst_db_filename = 'ALLP200.db'
    dst_db_path_2 = 'geomDB/'
    dst_db_filename_2 = 'geomDB_sqlite'
    tmp_path = 'tmp/'
    src_file   = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/sqlite200/ALLP200.db'
    src_file_2 = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/geomDB/geomDB_sqlite'
    #src_size = os.path.getsize(src_file)
    copy_start = time.time()
    if os.path.exists(scratch_path):
        try:
            if not os.path.exists(scratch_path + tmp_path):
                os.makedirs(scratch_path + tmp_path)
            if not os.path.exists(scratch_path + dst_db_path):
                os.makedirs(scratch_path + dst_db_path)
            copyfile(src_file, scratch_path + dst_db_path + dst_db_filename)
            if not os.path.exists(scratch_path + dst_db_path_2):
                os.makedirs(scratch_path + dst_db_path_2)
            copyfile(src_file_2, scratch_path + dst_db_path_2 + dst_db_filename_2)
            for files in inputfiles:
                copyfile(files[0],files[1])
        except:
            print 'Copy to scratch failed, execution terminated'
            return 1
    else:
        print 'Scratch directory dose not exist, execution terminated'
        return 1
        # true_sql_dir = '/lustre/atlas/proj-shared/csc108/ATLAS_DB_s1/21.0.15/DBRelease/current/sqlite200'
        # pseudo_sql_dir = "./sqlite200"
        # os.symlink(true_sql_dir, pseudo_sql_dir)

    true_dir = '/ccs/proj/csc108/AtlasReleases/21.0.15/nfs_db_files'
    pseudo_dir = "./poolcond"
    os.symlink(true_dir, pseudo_dir)

    copy_time = time.time() - copy_start
    localtime = time.asctime(time.localtime(time.time()))
    print("%s | %s Rank: %s | Node [ %s ]. Finished copying of DB to RAMdisk in %s sec." % (localtime, PanDA_jobId, rank, hostname, copy_time))

    #--------
    #pcmd = call(prep_comm, shell=True)
    #io_prep_finishtime = time.time()
    #localtime = time.asctime(time.localtime(io_prep_finishtime))
    #io_delay = io_prep_finishtime - io_prep_starttime
    # print("%s | %s Rank: %s | Node [ %s ]. Finished copying XML files in %s sec." % (localtime, PanDA_jobId, rank, hostname,io_delay))
    #---------

    payloadstdout = open(payloadstdout_s,"w")
    payloadstderr = open(payloadstderr_s,"w")
    
    localtime = time.asctime(time.localtime(time.time()))

    # Collect strace for debug

    #if rank == 1:
    #    my_command = strace + my_command
    '''
        env_fo = open(env_file, 'w')
        for k in os.environ.keys():
            l = "%s=%s\n" % (k, os.environ[k])
            env_fo.writelines(l)
        env_fo.close()
        syspath_fo = open(syspath_file,'w')
        for l in sys.path:
            syspath_fo.writelines("%s\n" % l)
        syspath_fo.close()
        with open(pathes, 'w') as p_file:
            p_file.writelines("PATH:\n")
            for p in os.environ['PATH'].split(':'):
                p_file.writelines("%s\n" % p)
            p_file.writelines("PYTHONPATH:\n")
            for p in os.environ['PYTHONPATH'].split(':'):
                p_file.writelines("%s\n" % p)
            p_file.writelines("LD_LIBRARY_PATH:\n")
            for p in os.environ['LD_LIBRARY_PATH'].split(':'):
                p_file.writelines("%s\n" % p)
    '''
    print("%s | %s Rank: %s Starting payload: %s" % (localtime, PanDA_jobId, rank, my_command))
    t0 = os.times()
    exit_code = call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    t = map(lambda x, y:x-y, t1, t0)
    t_tot = reduce(lambda x, y:x+y, t[2:3])

    payloadstdout.close()
    payloadstderr.close()

    report = open("rank_report.txt","w")
    report.write("cpuConsumptionTime: %s\n" % t_tot)
    report.write("exitCode: %s" % exit_code)
    report.close()
    
    # Finished all steps here
    localtime = time.asctime(time.localtime(time.time()))
    end_all = time.time()
    print("%s | %s Rank: %s | Node [ %s ]. Program run took  %s min. Exit code: %s cpuConsumptionTime: %s" % (localtime, PanDA_jobId,rank,hostname,(end_all-start_g)/60. ,exit_code, t_tot))

    return 0
    # Wait for all ranks to finish
    #comm.Barrier()

if __name__ == "__main__":
    sys.exit(main())
