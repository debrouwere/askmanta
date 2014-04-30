import os
import logging
import manta


logging.basicConfig()


if 'localhost' in os.environ['MANTA_URL']:
    client = manta.MantaClient(os.environ['MANTA_URL'], os.environ['MANTA_USER'], internal=True)
else:
    signer = manta.SSHAgentSigner(os.environ['MANTA_KEY_ID'])
    client = manta.MantaClient(os.environ['MANTA_URL'], os.environ['MANTA_USER'], signer)