class Stream:
    """
        All read stream should override '__enter__' and '__exit__',
        and the '__enter__' should return an iterable instance.
    """

    def __init__(self):
        self.open_state = False

    def is_open(self) -> bool:
        return self.open_state

    def __enter__(self):
        self.open_state = True
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.open_state = False


class KeyValueWriteStream(Stream):
    """
        All write stream should override '__enter__' and '__exit__',
        and the '__enter__' should return an instance with a 'write' method.
    """

    def write(self, key, value):
        assert self.open_state
