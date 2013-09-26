import yaml
import json
import itertools
import os
import tarfile
import operator


class Store(object):
    def __init__(self, job, name, dependencies, root='<root>'):
        path = "/tmp/askmanta/{job}/{name}.tar.gz".format(job=job, name=name)
        self.root = root
        self.name = name
        # self.archive = tarfile.open(path, 'w:gz')

    def add(self, item):
        if not item.startswith(self.root):
            raise ValueError("Can only add items to this store that are inside of the root: " + self.root)

        relpath = os.path.relpath(item, self.root)
        namespaced = os.path.join(relpath, self.name)
        self.archive.add(item, relpath)

    def create(self):
        self.archive.close()

        """
        Perhaps the easiest thing to do is to always install `askmanta` in the init of the first 
        step, and then run `askmanta emerge /:login/stor/jobs/:name` which will take care of 
        unpacking, installing dependencies etc.

        Maybe store this as an array (lines of bash instructions) so it's easy to append.
        """

    def upload(self):
        pass
        # assumes that we've already created 
        # the store (that way we can easily
        # support partial builds, stage steps
        # that don't redo the build etc.)


# only here to give subclasses a way to indicate that files 
# of this type need to be added to assets
class AssetStore(Store):
    pass


class PythonPackageStore(Store):
    """
    * create a temporary package directory
      tempfile.gettempdir() + 'askmanta/:jobname/:storename'
    * get requirements from spec
    * pip install -d $MANTA_PACKAGES -r $MANTA_REQUIREMENTS
    * <<tar>> $MANTA_PACKAGES
    """

    def add(self, item):
        print 'faux adding python package', item

    def create(self):
        # where did we write to?
        self.path = None
        # what are the init instructions to unpack this?
        self.instructions = None

    @classmethod
    def emerge(cls, file):
        """ We will call `askmanta emerge bla.tar.gz`
        and this method will take care of unpacking/
        installing/etc """


class FileStore(AssetStore):
    def create(self):
        # where did we write to?
        self.path = None

    @classmethod
    def emerge(cls, file):
        """ We will call `askmanta emerge bla.tar.gz`
        and this method will take care of unpacking/
        installing/etc. Before doing so askmanta emerge 
        will check if there's already an up-to-date extraction. """
        # - python: unzip
        # - python: untar
        # - sh: pip install --find-links {basepath} -r {basepath}/requirements.txt


"""
I don't like the emerge approach anymore. What I want is for the individual 
steps to be very direct. Maybe split out the functionality of Stores 
into (1) Requirements and (2) Store, where a store is a "utility" kind of 
class in case Requirements need to have things stores/uploaded/unpacked 
(which not all requirements do -- e.g. live-fetching package installers won't)

Dependencies provide per-step init instructions.
Store takes care of zipping everything up and unpacking it on the first step.

That said, both are of necessity very much entangled, because you can't provide 
per-step installation instructions if you don't know what you're getting from 
the Store.

Perhaps a Store should simply be a generic thing, one per platform, that 
can hold files if the Requirements so please, and will unpack on the first 
step.

Well... except not entirely generic. It still has to take care of 
merging requirements and the packaging steps might be different, e.g. 

    pip install -d $MANTA_PACKAGES -r $MANTA_REQUIREMENTS

for Python.

So you'd have a PythonPackageManifest and a PythonPackageStore, 
a FileManifest and a FileStore.
"""


platforms = {
    # 'apt': AptPackageStore, 
    'python': PythonPackageStore, 
    'files': FileStore, 
    'scripts': FileStore, 
    'inputs': FileStore, 
}