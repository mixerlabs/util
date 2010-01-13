import os
import glob
import posixpath
from stat import S_IMODE, S_ISDIR, S_ISREG
from subprocess import Popen, PIPE

def _zip_exec(args, cwd=None, ok=(0,)):
    process = Popen(args, cwd=cwd, stderr=PIPE, stdin=PIPE)
    (out, err) = process.communicate('')
    if process.returncode not in ok:
        raise Exception('%s error %d: %s' % (args[0], process.returncode, err))
    return process.returncode

def zips_to_dirs(zipfiles, output_base_dir=None, quiet=True):
    """ Converts zip files into directories.  If output_base_dir is provided,
    the source files are not destroyed.  Otherwise, this happens in-place.
    The directory names match the zip filenames exactly, as do the file
    permissions and times. """
    if isinstance(zipfiles, basestring):
        zipfiles = glob.glob(zipfiles)
    for zipfile in zipfiles:
        statinfo = os.stat(zipfile)
        if output_base_dir:
            outdir = posixpath.join(output_base_dir, 
                    posixpath.basename(zipfile))
        else:
            outdir = zipfile + '.tmp'
        flags = '-oq' if quiet else '-o'
        # Error 1 means no files were unzipped (empty zip file) - that's ok.
        _zip_exec(['unzip', flags, zipfile, '-d', outdir], ok=(0, 1))
        if not output_base_dir:
            os.remove(zipfile)
            os.rename(outdir, zipfile)
        os.chmod(zipfile, S_IMODE(statinfo.st_mode) | 0111)  # turn on x bits
        os.utime(zipfile, (statinfo.st_atime, statinfo.st_mtime))

def dirs_to_zips(dirs, quiet=True):
    """ Converts directories into zip files in-place.  The zip filename matches
    the directory name exactly, as do the file permissions and times.
    'dirs' can be an iterable of strings, or a single string with wildcards."""
    if isinstance(dirs, basestring):
        dirs = glob.glob(dirs)
    for dir in dirs:
        dir = dir.rstrip('/')
        statinfo = os.stat(dir)
        if not S_ISDIR(statinfo.st_mode):
            raise Exception('%s is not a directory' % dir)
        outfile = os.path.abspath('%s.tmp' % dir)
        flags = '-Rmq' if quiet else '-Rm'
        if _zip_exec(['zip', flags, outfile, '*'], cwd=dir,
                ok=(0, 12)) == 12:
            # Error 12 means no input files (empty dir).
            # We'll just create an empty zipfile directly instead.
            open(outfile, 'wb').write('PK\5\6' + '\0' * 18)
        os.rmdir(dir)
        os.rename(outfile, dir)
        os.chmod(dir, S_IMODE(statinfo.st_mode) ^ 0111)  # turn off execute bits

        os.utime(dir, (statinfo.st_atime, statinfo.st_mtime))
