VERSION = (0, 1, 0)

import functools
import json
import job, manifest

def phase(fn):
    @functools.wraps(fn)
    def wrapped_fn(data):
        input = json.loads(data)
        ret = fn(input)
        print json.dumps(ret)