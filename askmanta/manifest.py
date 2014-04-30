import os
import tarfile
import shutil
from gevent.pool import Pool
import subprocess
from askmanta.environment import client


class Store(object):
    def __init__(self, name, directive):
        self.directive = directive
        self.name = name
        # eventual location of the store
        self.basedir = self.directive.tmp
        self.filename = '{name}.tar.gz'.format(name=self.name)
        self.path = os.path.join(self.basedir, self.filename)
        self.cachepath = os.path.join(self.basedir, self.name)
        self.destination = os.path.join(directive.manta_tmp, self.name)
        self.archive_destination = os.path.join(directive.manta_root, self.filename)
        self.archive_asset = '/assets' + self.archive_destination
        # set of filenames
        self.files = set()

    def add(self, *files):
        self.files.update(files)

    def clear(self, directory=None):
        if not directory:
            directory = self.cachepath

        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory)

    def open(self):
        self.clear()
        self.archive = tarfile.open(self.path, 'w:gz')

    def close(self):
        self.archive.close()

    @property
    def is_active(self):
        return len(self.files) > 0


class Manifest(object):
    def __init__(self, name, phase):
        self.phase = phase
        self.directive = phase.directive
        self.root = self.directive.root
        self.name = name
        self.items = []
        self.init = []
        #self.assets = []

        # different manifests of the same type share 
        # a single store
        if name in self.directive.stores:
            self.store = self.directive.stores[name]
        elif hasattr(self, 'STORE_CLASS'):
            self.store = self.directive.stores[name] = self.STORE_CLASS(name, self.directive)

    def add(self, *items):
        self.items.extend(items)            

    def __repr__(self):
        return "<{cls}: {name}>".format(cls=self.__class__.__name__, name=self.name)


class FileStore(Store):
    def normalize_path(self, filename):
        if filename.startswith('/'):
            if filename.startswith(self.directive.root):
                message = "Can only write files to this store that are inside of the root: " + self.directive.root
                raise ValueError(message)
            else:
                return os.path.relpath(filename, self.directive.root)
        else:
            return filename

    def locate(self, filename):
        filename = self.normalize_path(filename)
        return os.path.join(self.directive.manta_root, filename)

    def save(self):
        self.open()
        for filename in self.files:
            absolute_path = os.path.join(self.directive.root, filename)
            relative_path = self.normalize_path(filename)
            namespaced_path = os.path.join('scripts', relative_path)
            self.archive.add(absolute_path, namespaced_path)
        self.close()


class FileManifest(Manifest):
    STORE_CLASS = FileStore

    def add(self, *items):
        self.items.extend(items)
        self.store.add(*items)
        #assets = [self.store.locate(item) for item in items]
        #self.assets.extend(assets)


class PythonPackageStore(Store):
    def save(self):
        self.open()

        cache = os.path.join(self.basedir, 'python')
        self.clear(cache)

        # add modules
        # TODO: a pool is pointless if we don't monkeypatch
        pool = Pool(4)
        download = lambda package: subprocess.call(["pip", "install", "-d", cache, package])
        pool.map(download, self.files)

        self.archive.add(cache, 'python')
        self.close()


# Manta comes with pip 1.2.1 out of the box, so installing 
# our packages is pretty straightforward.
class PythonPackageManifest(Manifest):
    STORE_CLASS = PythonPackageStore

    def add(self, *items):
        self.items.extend(items)
        for item in items:
            instruction = "pip install --find-links {path} {item}".format(
                path=self.store.destination, item=item)
            self.init.append(instruction)
        self.store.add(*items)



class AptPackageManifest(Manifest):
    def add(self, *items):
        self.items.extend(items)
        for item in items:
            instruction = "apt-get install {item}".format(item=item)
            self.init.append(instruction)


platforms = {
    'apt': AptPackageManifest, 
    'python': PythonPackageManifest, 
    'files': FileManifest, 
    'scripts': FileManifest, 
    'inputs': FileManifest, 
}
