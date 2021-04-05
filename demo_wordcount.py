from hadoop_lib import *
import os

current_path = os.getcwd()
print(current_path)

input_path = os.path.join(current_path, "demo_data", "test_input.txt")
output_path = os.path.join(current_path, "demo_data", "test_output.txt")


class WordCountMapper(Mapper):
    def map(self, key, value: str, context: HadoopContext):
        words = value.split()
        for word in words:
            context.write(word, 1)


class WordCountReducer(Reducer):
    def reduce(self, key, values, context: HadoopContext):
        context.write(key, sum(values))


job = HadoopJob()
job.set_mapper(WordCountMapper)
job.set_reducer(WordCountReducer)
hadoop_input = HadoopInput()
hadoop_output = HadoopOutput()
hadoop_input.set_input_paths(job, [input_path])
hadoop_output.set_output_path(job, output_path)

job.start_job()
print(job.is_successful())
