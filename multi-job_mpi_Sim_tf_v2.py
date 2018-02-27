#!/usr/bin/env python
import sys
sys.path=sys.path[1:]
from mpi4py import MPI
import os
import shutil
import time
from socket import gethostname
from subprocess import call
from glob import glob

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

    os.environ['PATH'] = ':'.join(path)
    os.putenv('PATH', ':'.join(path))
    os.environ['PYTHONPATH'] = ':'.join(ppath)
    os.putenv('PYTHONPATH', ':'.join(ppath))
    os.environ['LD_LIBRARY_PATH'] = ':'.join(ldpath)
    os.putenv('LD_LIBRARY_PATH', ':'.join(ldpath))

    # Cleanup of enviroment finished

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
    outputfiles = []
    subs_a = my_command.split()
    for i in range(len(subs_a)):
        if i > 0:
            if '(' in subs_a[i] and not subs_a[i][0] == '"':
                subs_a[i] = '"'+subs_a[i]+'"'
            if subs_a[i].startswith("--inputEVNTFile"):
                filename = subs_a[i].split("=")[1]
                inputfiles.append((filename, scratch_path + filename))
                subs_a[i] = subs_a[i].replace(filename, scratch_path + filename)
            if subs_a[i].startswith("--outputHITSFile"):
                outputfiles.append(subs_a[i].split("=")[1])

    my_command = ' '.join(subs_a)
    my_command = my_command.strip()
    my_command = my_command.replace('--DBRelease="all:current"', '') # avoid Frontier reading

    # create working directory for a given rank
    curdir = os.path.abspath(os.curdir)

    # wkdirname = "PandaJob_%s" % my_job_index

    wkdir_luster = os.path.abspath(os.path.join(curdir,wkdirname))
    if not os.path.exists(wkdir_luster):
        os.mkdir(wkdir_luster)

    # Go to local working directory (job rank)
    wkdir_ram = os.path.join(scratch_path, wkdirname)
    if not os.path.exists(wkdir_ram):
        os.mkdir(wkdir_ram)

    #os.chdir(wkdir)

    os.chdir(wkdir_ram)

    #---------
    # Copy Poolcond files to local working directory

    dst_db_path = 'sqlite200/'
    dst_db_filename = 'ALLP200.db'
    dst_db_path_2 = 'geomDB/'
    dst_db_filename_2 = 'geomDB_sqlite'
    tmp_path = 'tmp/'
    src_file   = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/sqlite200/ALLP200.db'
    src_file_2 = '/ccs/proj/csc108/AtlasReleases/21.0.15/DBRelease/current/geomDB/geomDB_sqlite'
    copy_start = time.time()
    if os.path.exists(scratch_path):
        try:
            if not os.path.exists(scratch_path + tmp_path):
                os.makedirs(scratch_path + tmp_path)
            if not os.path.exists(scratch_path + dst_db_path):
                os.makedirs(scratch_path + dst_db_path)
            shutil.copyfile(src_file, scratch_path + dst_db_path + dst_db_filename)
            if not os.path.exists(scratch_path + dst_db_path_2):
                os.makedirs(scratch_path + dst_db_path_2)
            shutil.copyfile(src_file_2, scratch_path + dst_db_path_2 + dst_db_filename_2)
            for files in inputfiles:
                shutil.copyfile(os.path.join(wkdir_luster, files[0]), files[1])
        except:
            print("Copy to scratch failed, execution terminated':  \n %s " % (sys.exc_info()[1]))
    else:
        print 'Scratch directory (%s) dose not exist, execution terminated' % scratch_path
        return 1

    true_dir = '/ccs/proj/csc108/AtlasReleases/21.0.15/nfs_db_files'
    pseudo_dir = "./poolcond"
    os.symlink(true_dir, pseudo_dir)

    copy_time = time.time() - copy_start
    localtime = time.asctime(time.localtime(time.time()))
    print("%s | %s Rank: %s | Node [ %s ]. Finished copying of DB to RAMdisk in %s sec." % (localtime, PanDA_jobId, rank, hostname, copy_time))

    payloadstdout = open(payloadstdout_s,"w")
    payloadstderr = open(payloadstderr_s,"w")

    '''
    if rank == 1:
        strace_log_path = "/lustre/atlas/proj-shared/csc108/pilots_workdir1/strace_log_%s.txt " % PanDA_jobId
        strace = 'strace -f -o ' + strace_log_path
        my_command = strace + my_command
    '''

    localtime = time.asctime(time.localtime(time.time()))

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

    # clean up of working dir and sym links
    clean_start =  time.time()
    try:
        os.unlink(pseudo_dir)
        for inputfile in inputfiles:
            remove(inputfile[1])
    except:
        print"%s | %s Something goes wrong with cleanup on: %s" % (localtime, PanDA_jobId, hostname)

    # moving of output files to luster
    for f in outputfiles:
        abs_path = os.path.join(wkdir_ram, f)
        if os.path.exists(abs_path):
            shutil.move(abs_path, wkdir_luster)

    # cleanup of working directory
    removeRedundantFiles(wkdir_ram)
    copytree(wkdir_ram, wkdir_luster)
    clean_time = time.time() - clean_start
    print("%s | %s Rank: %s | Node [ %s ]. Copy of output and logs to luster took: %s (sec.)" % (time.asctime(time.localtime(time.time())), PanDA_jobId, rank, hostname, clean_time))

    return 0


def removeRedundantFiles(workdir):
    """ Remove redundant files and directories """

    #print("Removing redundant files prior to log creation")

    workdir = os.path.abspath(workdir)

    dir_list = ["AtlasProduction*",
                "AtlasPoint1",
                "AtlasTier0",
                "buildJob*",
                "CDRelease*",
                "csc*.log",
                "DBRelease*",
                "EvgenJobOptions",
                "external",
                "fort.*",
                "geant4",
                "geomDB",
                "geomDB_sqlite",
                "home",
                "o..pacman..o",
                "pacman-*",
                "python",
                "runAthena*",
                "share",
                "sources.*",
                "sqlite*",
                "sw",
                "tcf_*",
                "triggerDB",
                "trusted.caches",
                "workdir",
                "*.data*",
                "*.events",
                "*.py",
                "*.pyc",
                "*.root*",
                "JEM",
                "tmp*",
                "*.tmp",
                "*.TMP",
                "MC11JobOptions",
                "scratch",
                "jobState-*-test.pickle",
                "*.writing",
                "pwg*",
                "pwhg*",
                "*PROC*",
                "madevent",
                "HPC",
                "objectstore*.json",
                "saga",
                "radical",
                "ckpt*"]

    # remove core and pool.root files from AthenaMP sub directories
    try:
        cleanupAthenaMP(workdir)
    except Exception, e:
        print("Failed to execute cleanupAthenaMP(): %s" % (e))

    # explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command (--dereference option)
    matches = []
    import fnmatch
    for root, dirnames, filenames in os.walk(workdir):
        for filename in fnmatch.filter(filenames, '*.a'):
            matches.append(os.path.join(root, filename))
    for root, dirnames, filenames in os.walk(os.path.dirname(workdir)):
        for filename in fnmatch.filter(filenames, 'EventService_premerge_*.tar'):
            matches.append(os.path.join(root, filename))
    if matches != []:
        #print("Encountered %d archive files - will be purged" % len(matches))
        #print("To be removed: %s" % (matches))
        for f in matches:
            remove(f)
    #else:
    #    print("Found no archive files")

    # note: these should be partitial file/dir names, not containing any wildcards
    exceptions_list = ["runargs", "runwrapper", "jobReport", "log."]

    for _dir in dir_list:
        files = glob(os.path.join(workdir, _dir))
        exclude = []

        # remove any dirs/files from the exceptions list
        if files:
            for exc in exceptions_list:
                for f in files:
                    if exc in f:
                        exclude.append(os.path.abspath(f))

            #if exclude != []:
            #    print('To be excluded from removal: %s' % (exclude))

            _files = []
            for f in files:
                if not f in exclude:
                    _files.append(os.path.abspath(f))
            files = _files

            #print("To be removed: %s" % (files))
            for f in files:
                remove(f)

    # run a second pass to clean up any broken links
    broken = []
    for root, dirs, files in os.walk(workdir):
        for filename in files:
            path = os.path.join(root,filename)
            if os.path.islink(path):
                target_path = os.readlink(path)
                # Resolve relative symlinks
                if not os.path.isabs(target_path):
                    target_path = os.path.join(os.path.dirname(path),target_path)
                if not os.path.exists(target_path):
                    broken.append(path)
            else:
                # If it's not a symlink we're not interested.
                continue

    if broken:
        #print("Encountered %d broken soft links - will be purged [%s]" % (len(broken),broken))
        for p in broken:
            remove(p)
    #else:
    #    print("Found no broken links")

    return 0


def cleanupAthenaMP(workdir):
    """ Cleanup AthenaMP sud directories prior to log file creation """

    for ampdir in glob('%s/athenaMP-workers-*' % (workdir)):
        #print("Attempting to cleanup %s" % (ampdir))
        for (p, d, f) in os.walk(ampdir):
            for filename in f:
                if 'core' in filename or 'pool.root' in filename or 'tmp.' in filename or '':
                    path = os.path.join(p, filename)
                    path = os.path.abspath(path)
                    #print("Cleaning up %s" % (path))
                    remove(path)

    return 0


def remove(path):

    path = os.path.abspath(path)
    try:
        os.unlink(path)
    except OSError as e:
        print("Problem with deletion: %s : %s [%s]" % (e.errno, e.strerror, path))
        return 1
    return 0


def copytree(src, dst, symlinks=False, ignore=None):

    src = os.path.abspath(src)
    dst = os.path.abspath(dst)

    try:
        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, symlinks, ignore)
            else:
                shutil.copy2(s, d)
    except OSError as e:
        print("Something goes wrong with copy")

    return 0


if __name__ == "__main__":
    sys.exit(main())
