import argparse
import os
import askmanta
import job, stor
import yaml
from askmanta.environment import client


def get_directive(src):
    spec = yaml.load(src)
    filename = os.path.basename(src.name)
    name = os.path.splitext(filename)[0]
    root = os.path.dirname(os.path.join(os.getcwd(), src.name))
    return askmanta.job.Directive(name, spec, root) 


def parse():
    arguments = argparse.ArgumentParser()
    subparsers = arguments.add_subparsers()

    build = subparsers.add_parser('build',
        help="build step file, zip up files etc. but don't submit")
    build.set_defaults(func=job.build)

    stage = subparsers.add_parser('stage', 
        help="upload a build, but don't run yet")
    stage.set_defaults(func=job.stage)

    submit = subparsers.add_parser('submit', 
        help="just create the job, don't worry about dependencies, useful for recurring jobs")
    submit.set_defaults(func=job.submit)

    run = subparsers.add_parser('run',
        help="build, stage and submit the job")
    run.set_defaults(func=job.run)

    for command in [build, stage, submit, run]:
        command.add_argument('src', type=file)

    for command in [build, run]:
        command.add_argument('-s', '--simple', 
            default=False, action='store_true',
            help="simple builds assume code might have changed but dependencies have not; equal to `--partial scripts`")        
        command.add_argument('-p', '--partial', 
            default=False, action='store_true',
            help="only (re)build one or more of: files, scripts, python (dependencies)")  
        command.add_argument('-f', '--fresh', 
            default=False, action='store_true',
            help="delete any and all existing remote files related to this job")  

    for command in [submit, run]:
        command.add_argument('-i', '--input', 
            nargs='+', 
            help="The input files (on Manta) on which your job should run.")
        command.add_argument('-t', '--test', 
            default=False, action='store_true', 
            help="Do a test run and don't actually submit the job.")
        command.add_argument('-w', '--watch', 
            default=False, nargs='?', type=int, 
            help="wait for job to complete, checking every n seconds for an update, and return a status report for every step that completes")
        command.add_argument('-o', '--cat-outputs',
            default=False, action='store_true',
            help="will return output; this also implies -w")
        command.add_argument('-d', '--discard',
            default=False, action='store_true',
            help="deletes all information about the job after it's run, depends on -w or -o")

    status = subparsers.add_parser('status', 
        help="gives an overview of job status, if there's any errors, downloads those and displays them, etc.")
    status.set_defaults(func=stor.status)
    status.add_argument('id', nargs='?')

    ls = subparsers.add_parser('ls')
    ls.set_defaults(func=stor.ls)
    ls.add_argument('-l', '--long',
        default=False, action='store_true',
        help="sort jobs by job name and list various metadata")

    rm = subparsers.add_parser('rm', 
        help="remove job files")
    rm.set_defaults(func=stor.rm)
    rm.add_argument('id', nargs='?', default=None)
    rm.add_argument('-d', '--delta',
        default=False, action='store_true',
        help="wipe all jobs older than n days")


    args = arguments.parse_args()

    if hasattr(args, 'src'):
        directive = get_directive(args.src)
        args.func(directive, args)
    elif hasattr(args, 'id'):
        active_job = askmanta.job.Job(id=args.id)
        args.func(active_job, args)
    else:
        args.func(args)

