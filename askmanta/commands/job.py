import yaml
import json
import itertools
import os
import tarfile
import operator
import time
import askmanta
from askmanta.environment import client


def build(directive, args):
    directive.build()
    # print json.dumps(directive.serialize(), indent=4)


def stage(directive, args):
    directive.stage()


def submit(directive, args):
    if args.test:
        print json.dumps(directive.serialize(), indent=4)
        return

    job = directive.submit(args.input) 
    print 'launched job', job.id

    if args.watch is not False:
        while not job.is_done:
            time.sleep(args.watch or 5)
            job.poll()

        if job.errors:
            print 'job finished with errors\n'
            for resource in job.errors:
                print resource.content
        else:
            print 'job finished'

            if (args.cat_output):
                for resource in job.outputs:
                    print resource.content


def run(directive, args):
    """ Running a job means building, staging and submitting it. """

    if args.discard and not (args.watch or args.cat_output):
        raise ValueError("Can only discard intermediate job data when watching the job.")

    if args.test:
        print json.dumps(directive.serialize(), indent=4)
        return

    build(directive, args)
    stage(directive, args)
    submit(directive, args)
