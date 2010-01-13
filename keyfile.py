import hashlib
import os.path
from M2Crypto import X509, SMIME

from environment import env
from util.functional import memoize
from util.files import mixer_path

@memoize
def id_from_cert():
    """Get the ID of the currently loaded certificate. This is an
    opaque string uniquely identifying the loaded EC2 certificate."""
    cert = X509.load_cert(env.ec2.cert)
    rsa = cert.get_pubkey().get_rsa()
    m = hashlib.md5()
    m.update(rsa.e)
    m.update(rsa.n)

    return m.hexdigest()

def keyfile_path(directory, name, ext='yaml'):
    """Construct a path to the keyfile named by the `directory' and
    `name'. `directory' is relative to the source root. Optionally specify
    an extension."""
    return os.path.join(
        mixer_path(directory),
        '%s-%s.%s.smime' % (id_from_cert(), name, ext)
    )

@memoize
def get_keyfile(directory, name, ext='yaml'):
    """Returns the DECRYPTED keyfile named by the given `directory',
    `name' and `ext' (as passed to ``keyfile_path'')."""
    s = SMIME.SMIME()
    s.load_key(env.ec2.pk, env.ec2.cert)
    p7, data = SMIME.smime_load_pkcs7(keyfile_path(directory, name, ext))

    return s.decrypt(p7)
