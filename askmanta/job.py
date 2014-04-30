import os
import askmanta
import json
from dateutil import parser as dateparser
from askmanta.environment import client


class Phase(object):
    # phase name = "job name: phase i"

    def __init__(self, i, spec, directive):
        self.i = i
        self.spec = spec
        self.directive = directive
        self.type = spec.get('type', 'map')
        self.init = spec.get('init', [])
        self.manifests = []
        self.assets = spec.get('assets', [])
        self.count = None
        self.memory = None
        self.disk = None

        if 'sh' in spec:
            self.executable = spec['sh']
        elif 'script' in spec:
            path = os.path.join(self.directive.manta_tmp, 'scripts', spec['script'])
            self.executable = path
            spec.setdefault('dependencies', {}).setdefault('scripts', []).append(spec['script'])
        else:
            raise ValueError()

        platforms = spec.get('dependencies', False)
        if platforms:
            for platform, dependencies in platforms.items():
                manifest = askmanta.manifest.platforms[platform](platform, self)
                manifest.add(*dependencies)
                self.manifests.append(manifest)

    @property
    def name(self):
        return "{directive}: step {i}/{n}".format(
            directive=self.directive.name, i=self.i, n=len(self.directive))

    def serialize(self):
        assets = self.assets
        init = self.init

        for manifest in self.manifests:
            init = init + manifest.init

        for store in self.directive.stores.values():
            assets = assets + [store.archive_destination]

        instruction = {
            'type': self.type,
            'init': ";\n".join(init), 
            'assets': assets, 
            'exec': self.executable, 
        }

        for key in ['count', 'memory', 'disk']:
            option = getattr(self, key)
            if option:
                instruction[key] = option

        return instruction


class Directive(object):
    def __init__(self, name, spec, root):
        self.name = name
        self.spec = spec
        # local root
        self.root = root
        # manta root
        self.manta_root = "/{account}/stor/directives/{name}".format(
            account=client.account, name=name)
        self.tmp = "/tmp/askmanta/{name}".format(name=self.name)
        self.manta_tmp = "/var/tmp"
        self.stores = {}
        self.parse()

    def parse(self):
        self.phases = [Phase(i, spec, self) for i, spec in enumerate(self.spec)]
        for store in self.stores.values():
            if store.is_active:
                instruction = "cd /var/tmp && tar xvzf {src}".format(
                    src=store.archive_asset)
                for phase in self.phases:
                    phase.init.insert(0, instruction)

    def serialize(self):
        return [phase.serialize() for phase in self.phases]

    def build(self):
        for store in self.stores.values():
            if store.is_active:
                store.save()

        phases_filename = os.path.join(self.tmp, 'phases.json')
        json.dump(self.serialize(), open(phases_filename, 'w'), indent=4)

    def stage(self):
        # TODO: support for -f --fresh, which would client.rmr(base) first
        client.mkdirp(self.manta_root)

        # TODO: client.get_object(name), check `mtime` > self.store.ctime and if so, abort
        # (current version already uploaded)
        for store in self.stores.values():
            if not store.is_active:
                continue

            print store.archive_destination
            client.put_object(
                store.archive_destination, 
                file=open(store.path), 
                durability_level='1', 
                )

    def submit(self, inputs):
        job_id = client.create_job(self.serialize(), self.name)
        client.add_job_inputs(job_id, inputs)
        client.end_job_input(job_id)
        return Job(id=job_id)

    def run(self, inputs):
        self.build()
        self.stage()
        return self.submit(inputs=inputs)

    def local_run(self, inputs):
        """
        * create a virtualenv for every step, with just the packages for that step
        * emulate how Joyent pipes lines into stdin
        * local runs can't really work if the phases have side-effects, but if 
          they don't and if the input files are local too, things should work swimmingly
        """
        raise NotImplementedError()

    def __len__(self):
        return len(self.spec)



class File(object):
    def __init__(self, path):
        self.path = path

    @property
    def content(self):
        if not hasattr(self, '_content'):
            self._content = client.get_object(self.path)
        return self._content

    def json(self):
        return json.loads(self.content)


class Job(object):
    # we can initialize a job with either an id 
    # (for an existing job) or a directive (for
    # a job that's already running)
    def __init__(self, id=None, directive=None):
        self.id = id
        self.directive = directive
        self.root = os.path.join('/', client.account, 'jobs', id)
        # TODO: distinguish between live and archived jobs
        #self.path = os.path.join(self.root, 'live/status')
        self.path = os.path.join(self.root, 'job.json')
        self.errors = []
        self.outputs = []
        self.is_done = None

    def poll(self):
        self.raw = raw = File(self.path).json()
        self.name = raw['name']
        self.state = raw['state']
        self.is_done = raw['state'] == 'done'
        self.stats = raw['stats']
        current = raw['stats']['tasksDone']
        total = raw['stats']['tasks']
        self.cursor = (current, total)
        self.ctime = dateparser.parse(self.raw['timeCreated'])
        if self.stats['errors']:
            try:
                # TODO: distinguish between live and archived jobs
                # live/err => err.txt
                overview_path = os.path.join(self.root, 'live/err')
                overview = File(overview_path).json()
                stderr_path = overview['stderr']
                stderr = File(stderr_path)
                self.errors.append(stderr)
            except:
                pass

        if self.stats['outputs']:
            pass

    def delete(self):
        # too lazy to implement this in Python...
        subprocess.call(["mrm", "-r", self.root])