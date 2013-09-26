
"""
Similarly, I need to think about jobs and runs. The ideal metaphor is 
"a job, and a run of that job", but this would clash with Manta which 
calls an individual run a "job".

Maybe talk about JobTemplates instead. Or Recipes? Or Manuals? Or scripts?

    We've run three jobs with the MakeThumbnails directive.

Instruction / directive / directions / is the only thing I can think of that doesn't mix metaphors, 
and "directive" the only thing that works in singular.



class Job(object):
    has_one directive

Jobs can be in different states. They can exist only locally, yet to be submitted, 
they can be running live on compute instances, they can have finished, etc.
"""


class Directive(object):
    pass


class Job(object):
    def _aggregate_dependencies(self):
        dependency_groups = {
            'global': [self.raw.get('dependencies', {})], 
            'local': [phase.get('dependencies', {}) or {} for phase in self.phases], 
            'script': [dict(scripts=filter(None, [phase.get('script')])) for phase in self.phases], 
            }
        dependency_lists = reduce(operator.add, dependency_groups.values())

        # at this point we have a list of several dependency declarations, 
        # which we now have to merge and deduplicate.
        deduped = {}
        for dependency_list in dependency_lists:
            for platform, dependencies in dependency_list.items():
                deduped.setdefault(platform, set()).update(dependencies)

        self.dependencies = deduped
        self.uploads = [STORES[processor](self.name, processor, dependencies) 
            for processor, dependencies in self.dependencies.items()]

    def _translate_phases(self):
        self.instructions = instructions = []

        for phase in self.phases:
            instruction = {'init': [], 'assets': []}

            if 'sh' in phase:
                instruction['exec'] = phase['sh']
            elif 'script' in phase:
                base = '/assets/{account}/stor/jobs/{name}/scripts/'.format(
                    account=self.client.account, name=self.name)
                path = os.path.join(base, phase['script'])
                instruction['exec'] = path
            else:
                raise ValueError()

            instruction['init'] = ['~ askmanta emerge ~']

            assets = instruction['assets']
            dependency_groups = phase.get('dependencies', False)
            if dependency_groups:
                for platform, dependencies in dependency_groups.items():
                    if issubclass(STORES[platform], AssetStore):
                        for dependency in dependencies:
                            base = '/assets/{account}/stor/jobs/{name}/{platform}/'.format(
                                account=self.client.account, name=self.name, platform=platform)
                            assets.append(os.path.join(base, dependency))

            instructions.append(instruction)

        # TODO
        # add initial unpacking to first phase's init
        instructions[0]['init'].insert(0, "~ install pip ~ install askmanta ~ askmanta emerge ~")

        # TODO
        # add the appropriate initialization to each phase

    def __init__(self, client, name, spec, root):
        self.client = client
        self.name = name
        self.raw = spec
        self.inputs = {
            'manta': [], 
            'local': [],
            }
        self.inputs.update(spec.get('inputs', {}))
        self.phases = spec.get('phases', [])
        self._aggregate_dependencies()
        self._translate_phases()

        from pprint import pprint
        # pprint(self.dependencies)
        pprint(self.instructions)



def build(client, args):
    if (args.simple or args.partial) and args.fresh:
        raise ValueError("Fresh builds cannot be partial.")

    """
    Assemble dependencies: 

    - scripts.tar.gz
    - <platform>.tar.gz
      (files, python)

    Create a `phases.json` file (for debugging purposes)
    - every platform is responsible for detailing what it needs
      to have available in `init` and `assets`
    - every platform can only specify a single directory, which
      will then be tarred and gzipped
    - scripts need to have a transformation applied whereby...
      - add all assets to /:login/stor/jobs/:jobname
      - then add /assets/:file to `assets` for each file
    """

    instructions = []
    
    spec = yaml.load(args.src)
    filename = os.path.basename(args.src.name)
    name = os.path.splitext(filename)[0]
    root = os.path.dirname(os.path.join(os.getcwd(), args.src.name))
    Job(client, name, spec, root)

def stage(client, args):
    # PSEUDOCODE

    base = '/{account}/stor/jobs/{job}/'.format(account=client.account, job='todo')
    # TODO: support for -f --fresh, which would client.rmr(base) first
    client.mkdirp(base)
    for store in stores:
        # TODO: client.get_object(name), check `mtime` > self.store.ctime and if so, abort
        # (current version already uploaded)
        client.put_object(base + self.store.name, file=open(self.store.filename), durability_level=1)
    pass

def submit(client, args):
    """
    phases = json.load(open('home.json'))
    job_id = client.create_job(phases, 'home job')
    print 'created job, id:', job_id
    client.add_job_inputs(job_id, ['/stdbrouw/stor/catalog.csv'])
    client.end_job_input(job_id)
    """
    pass

def run(client, args):
    if args.discard and not (args.watch or args.cat_output):
        raise ValueError("Can only discard intermediate job data when watching the job.")





class Job(object):
    def __init__(self, client, id, mtime=None):
        self.client = client
        self.id = id
        self.mtime = mtime
        self.raw = None

    def load(self):
        path = "/{}/jobs/{}/job.json".format(self.client.account, self.id)
        data = self.client.get_object(path)
        self.raw = json.loads(data)

    def update(self):
        self.load()

    def describe(self):
        if not self.raw:
            self.load()

        current = self.raw['stats']['tasksDone']
        total = self.raw['stats']['tasks']

        return {
            'id': self.id,
            'name': self.raw['name'], 
            'task': current, 
            'tasks': total, 
            'phase': "{}/{}".format(current, total), 
            'ctime': parse(self.raw['timeCreated']).strftime("%Y/%m/%d %H:%M"), 
            #'mtime': parse(self.mtime).strftime("%Y/%m/%d %H:%M"), 
            'errors': self.raw['stats']['errors'],
        }

    def pluck(self, *keys):
        desc = self.describe()
        plucked = []
        for key in keys:
            plucked.append(desc[key])
        return plucked
