"""Various I/O utilities."""

class GeneratorFile(object):
    """Provides a limited virtual file interface (only read()) where
    the contents is streamed (and buffered) as needed via generator."""
    def __init__(self, more, default_read_size=1024):
        """`more' is a generator that must produce string
        objects. `default_read_size' is the amount of bytes we attempt to
        return when read() is called without any arguments."""
        self.more              = more
        self.buffer            = ''
        self.default_read_size = default_read_size
        self.stopped           = False
        self.pos               = 0

    def _fill(self, howmuch):
        if howmuch < 0:
            howmuch = self.default_read_size

        try:
            while howmuch > 0:
                # In the future we could use two-way generators to
                # pass a hint as to how much data we want. Though
                # likely the receiving generator is "nicely" chunked
                # anyway.
                this = self.more.next()
                howmuch -= len(this)
                self.buffer += this
        except StopIteration:
            self.stopped = True

    def _peel(self, howmuch):
        if howmuch < 0:
            howmuch = self.len()

        this        = self.buffer[:howmuch]
        self.buffer = self.buffer[howmuch:]
        self.pos   += howmuch

        return this

    def len(self):
        return len(self.buffer)

    def empty(self):
        return self.len() == 0

    #  read(...)
    #      read([size]) -> read at most size bytes, returned as a string.
    #      
    #      If the size argument is negative or omitted, read until EOF
    #      is reached.  Notice that when in non-blocking mode, less
    #      data than what was requested may be returned, even if no
    #      size parameter was given.
    def read(self, n=-1):
        if n < 0 and self.empty():
            self._fill(self.default_read_size)
        elif self.len() < n:
            self._fill(n)

        return self._peel(n)

    #  readline(...)
    #      readline([size]) -> next line from the file, as a string.
    #      
    #      Retain newline.  A non-negative size argument limits the
    #      maximum number of bytes to return (an incomplete line may
    #      be returned then).  Return an empty string at EOF.
    def readline(self, n=-1):
        while True:
            pos = self.buffer.find('\n')
            if pos >= 0:
                # 0-indexed, we need one more byte.
                howmuch = pos + 1

                if n >= 0:
                    howmuch = min(howmuch, n)

                return self._peel(howmuch)
            elif 0 <= n <= self.len():
                return self._peel(n)
            elif self.stopped:
                return self._peel(self.len())
            else:
                self._fill(n)

    #  tell(...)
    #      tell() -> current file position, an integer (may be a long
    #      integer).

    def tell(self):
        return self.pos

    #  seek(...)
    #      seek(offset[, whence]) -> None.  Move to new file position.
    #      
    #      Argument offset is a byte count.  Optional argument whence
    #      defaults to 0 (offset from start of file, offset should be
    #      >= 0); other values are 1 (move relative to current
    #      position, positive or negative), and 2 (move relative to
    #      end of file, usually negative, although many platforms
    #      allow seeking beyond the end of a file).  If the file is
    #      opened in text mode, only offsets returned by tell() are
    #      legal.  Use of other offsets causes undefined behavior.
    #      Note that not all file objects are seekable.
    #
    # NOTE: we support only seek(0), and that only when pos == 0
    # already.

    def seek(self, offset):
        if offset != 0:
            raise ValueError, 'GeneratorFiles do not support non-zero offsets!'
        if self.pos != 0:
            raise ValueError, (
                'GeneratorFiles do not support seeking after reads!'
            )

    def write(self, *args):
        assert False, 'Not supported!'
