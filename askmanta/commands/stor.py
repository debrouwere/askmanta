# encoding: utf-8

import json
import itertools
from prettytable import PrettyTable
from dateutil.parser import parse
from askmanta.job import Job
from askmanta.environment import client

# TODO: should also get errors (if any) etc.
# (ideally implement this as a pure API function which we can then 
# use in different contexts)
# TODO: if status is asked for without a job, display 
# all running jobs, their phase and any errors (== ls -a)
def status(job, args):
    job.poll()
    phase = "/".join(map(str, job.cursor))
    errors = job.stats['errors']
    time = job.ctime.strftime("%y/%m/%d %H:%M")

    table = PrettyTable(['phase', 'errors', 'id', 'created'])
    table.add_row([phase, errors, job.id, time])

    print table


def ls(args):
    directory = "/{}/jobs/".format(client.account)
    jobs = client.list_directory(directory)

    if args.long:
        def fetch(jobfile):
            job = Job(jobfile['name'])
            job.poll()
            return job

        jobfiles = map(fetch, jobs)
        jobs = itertools.groupby(jobfiles, lambda job: job.name)

        for name, runs in jobs:
            table = PrettyTable(['phase', 'errors', 'id', 'created'])
            for run in runs:
                phase = "/".join(map(str, run.cursor))
                errors = run.stats['errors']
                time = run.ctime.strftime("%y/%m/%d %H:%M")
                table.add_row([phase, errors, run.id, time])
            length = len(unicode(table).split('\n')[0])
            print name.center(length)
            print table
    else:
        table = PrettyTable(['id', 'modified'])
        for job in jobs:
            table.add_row([job['name'], job['mtime']])
        print table


def rm(job, args):
    pass