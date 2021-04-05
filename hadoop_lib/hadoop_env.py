import os
from .stream import *


class HadoopContext(Stream):
    """
        The 'HadoopContext' is readable and writable, and can read and write simultaneously.
    """

    def __init__(
        self,
        config: dict,
        key_value_input_stream: Stream,
        key_value_output_stream: KeyValueWriteStream,
    ):
        self.config = config
        self.key_value_input_stream = key_value_input_stream
        self.key_value_output_stream = key_value_output_stream

    def write(self, key, value):
        assert self.key_value_output_stream.is_open()
        self.key_value_output_stream.write(key, value)

    def get_configuration(self) -> dict:
        return self.config

    def __enter__(self):
        """
            Open reading and writing stream simultaneously.
            This function will return a iterable instance for reading. For writing, 
            using 'HadoopContext.write' after calling this function.
        """
        super().__enter__()
        self.key_value_output_stream.__enter__()
        return self.key_value_input_stream.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
            Close reading and writing stream.
        """
        super().__exit__(exc_type, exc_val, exc_tb)
        self.key_value_input_stream.__exit__(exc_type, exc_val, exc_tb)
        self.key_value_output_stream.__exit__(exc_type, exc_val, exc_tb)


class Mapper:
    def __init__(self):
        pass

    def setup(self, context: HadoopContext):
        return

    def map(self, key, value, context: HadoopContext):
        raise NotImplementedError

    def cleanup(self, context: HadoopContext):
        return


class Reducer:
    def __init__(self):
        pass

    def setup(self, context: HadoopContext):
        return

    def reduce(self, key, values, context: HadoopContext):
        raise NotImplementedError

    def cleanup(self, context: HadoopContext):
        return


class ShuffleStream(Stream):
    """
        We simply use a dict to simulate shuffle process in Hadoop

        The 'ShuffleStream' is readable and writable like 'HadoopContext', but can't read
        and write simultaneously, i.e. write phase -> read pahse. For memory efficiency, we
        delete all stream data when reading phase is finished.
    """

    def __init__(self):
        self.shuffle_pair = dict()
        self.__write_phase = True

    def write(self, key, value):
        if key in self.shuffle_pair:
            self.shuffle_pair[key].append(value)
        else:
            self.shuffle_pair[key] = [value]

    def __enter__(self):
        """
            In the writing phase, '__enter__' return an instance with a 'write' mehod,
            and in the reading phase, '__enter__' return an iterable instance.
        """
        super().__enter__()
        if self.__write_phase:
            return self
        else:
            return self.shuffle_pair.items().__iter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        if self.__write_phase:
            self.__write_phase = False
        else:
            self.shuffle_pair = dict()  # delete 'shuffle_pair'


class HadoopJob:
    def __init__(self, config: dict = None):
        self.config = dict()
        self.mapper = None
        self.reducer = None
        self.input_stream = None
        self.shuffle_stream = None
        self.output_stream = None
        self.map_context = None
        self.reduce_context = None

        self.successful = False

        if config is not None:
            self.config.update(config)

        self.shuffle_stream = ShuffleStream()

    def set_mapper(self, mapper_cls):
        self.mapper = mapper_cls()
        assert isinstance(self.mapper, Mapper)

    def set_reducer(self, reducer_cls):
        self.reducer = reducer_cls()
        assert isinstance(self.reducer, Reducer)

    def start_job(self):
        self.__start_check()

        # map phase and shuffle phase
        with self.map_context as opened_map_context:
            self.mapper.setup(self.map_context)
            for key, value in opened_map_context:
                self.mapper.map(key, value, self.map_context)
            self.mapper.cleanup(self.map_context)

        with self.reduce_context as opened_reduce_context:
            self.reducer.setup(self.reduce_context)
            for key, values in opened_reduce_context:
                self.reducer.reduce(key, values, self.reduce_context)
            self.reducer.cleanup(self.reduce_context)

        self.successful = True

    def is_successful(self) -> bool:
        return self.successful

    def set_input_stream(self, input_stream):
        self.input_stream = input_stream
        self.map_context = HadoopContext(
            self.config, self.input_stream, self.shuffle_stream
        )

    def set_output_stream(self, output_stream):
        self.output_stream = output_stream
        self.reduce_context = HadoopContext(
            self.config, self.shuffle_stream, self.output_stream
        )

    def __start_check(self):
        assert self.mapper is not None
        assert self.reducer is not None
        assert self.config is not None
        assert self.input_stream is not None
        assert self.output_stream is not None
        assert self.shuffle_stream is not None
        assert self.map_context is not None
        assert self.reduce_context is not None


class HadoopInput(Stream):
    """
        For simplicity, we implement input stream using HadoopInput itself.
        The stream should implement '__enter__' and '__exit__', which is required by keyword 'with'.
    """

    def __init__(self):
        self.paths = None

        def default_format_func(line_id: int, line: str) -> tuple:
            return line_id, line.strip()

        self.format_func = default_format_func

    def set_format_func(self, format_func):
        """
            Set the input format function.
            By default, the 'HadoopInput' will format every line in input with (line_id, line),
            where the line_id is key, and the line is value, which will be inputted to the Mapper.

            the 'format_func' should like 'func(line_id: int, line: str) -> tuple', i.e. input
            line_id and line, and output (key, value) tuple.
        """
        self.format_func = format_func

    def set_input_paths(self, job: HadoopJob, paths: tuple):
        self.paths = paths
        assert len(paths) > 0
        for path in paths:
            assert os.path.exists(path)
        job.set_input_stream(self)

    def __enter__(self):
        """
            We employ 'yield' in '__get_assembled_input_stream' to make a generator,
            and do open and close file there, so we do nothing in '__enter__' and '__exit__'.
        """
        super().__enter__()
        return self.__get_assembled_input_stream()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

    def __get_assembled_input_stream(self):
        current_line = 0
        for path in self.paths:
            with open(path, "r", encoding="utf-8") as file:
                for line in file:
                    yield self.format_func(current_line, line)
                    current_line += 1


class HadoopOutput(KeyValueWriteStream):
    """
        For simplicity, we implement output stream using HadoopOutput itself.
        The stream should implement 'write' method to save reduce output(see 'HadoopOutput.write'),
        and also should implement '__enter__' and '__exit__', which is required by keyword 'with'.
    """

    def __init__(self):
        self.path = None

        def default_format_func(key, value) -> str:
            return "{} {}\n".format(str(key), str(value))

        self.format_func = default_format_func
        self.__file_stream = None

    def set_format_func(self, format_func):
        """
            Set the output format function.
            By default, the 'HadoopOutput' will simply output (key, value) to one line, and split them
            with a space.
            the 'format_func' should like 'func(key, value) -> str', i.e. input
            key and value, and output formatted string.
            'HadoopOutput' will not add '\n' to your 'format_func' output, you can add it in your
            function if needed.
        """
        self.format_func = format_func

    def set_output_path(self, job: HadoopJob, path: str):
        self.path = path
        job.set_output_stream(self)

    def write(self, key, value):
        assert self.__file_stream is not None
        assert self.is_open()
        self.__file_stream.write(self.format_func(key, value))

    def __enter__(self):
        super().__enter__()
        self.__file_stream = open(self.path, mode="w", encoding="utf-8").__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.__file_stream.__exit__(exc_type, exc_val, exc_tb)

