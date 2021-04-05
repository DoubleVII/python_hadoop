# 说明文档

## 搭建基本的MapReduce任务

我们以本项目提供的demo（demo_wordcount）为例，WordCount任务要求统计文本中每个单词出现的次数。
输入的文本由多行组成，每行为一个句子，输出的结果每行由一个单词和它的频率组成，如"apple 4"。

### 创建Mapper
为了自定义map操作，需要创建一个类，并继承`hadoop_lib.Mapper`，然后重写其`map`方法。
```python
class WordCountMapper(Mapper):
    def map(self, key, value: str, context: HadoopContext):
        words = value.split()
        for word in words:
            context.write(word, 1)
```

`map`方法接收三个参数，key、value和contex，，在默认的情况下，key是一个整型id（通常是输入文件的行号），value是输入文件中key对应的一行文本，contex是Hadoop环境提供的上下文对象。

这里value就是每一行文本，我们将其分割为多个单词，组成新的key-value，即单词作为key，单词出现的次数（1次）作为value，然后调用`contex.write`方法，写入新的key-value。

### 创建Reducer

为了自定义reduce操作，需要创建一个类，并继承hadoop_lib.Reducer，然后重写其reduce方法。
```python
class WordCountReducer(Reducer):
    def reduce(self, key, values, context: HadoopContext):
        context.write(key, sum(values))

```

`reduce`方法接收三个参数，key、values和contex，key是map函数调用`context.write`方法写入的key值。在Shuffle阶段，Hadoop环境会将相同的key值对应的value合并到一起，作为values输入到`reduce`方法中。

在这个例子中，key对应map阶段写入的word，values是该word对应的value。比如`key='apple', values=[1,1,1,1]`。在`reduce`中，将values相加，得到单词的频率，然后写入contex中。reduce阶段写入到contex的key-value最终会写入到结果文件中，默认每行为一个键值对，key与value以空格分隔。

另外，这里的values是一个可迭代对象（iterable），这意味这你可以通过for循环遍历它，但是不能通过下标引用其中的值。

### 创建Hadoop任务
创建一个Hadoop任务，只需要实例化一个`HadoopJob`对象，并为其设置Mapper类和Reducer类。

```python
job = HadoopJob()
job.set_mapper(WordCountMapper)
job.set_reducer(WordCountReducer)
```

### 设置输入和输出路径
为了设置Hadoop任务的输入和输出，需要实例化`HadoopInput`和`HadoopOutput`对象，然后调用`HadoopInput.set_input_paths`方法设置输入路径，调用`HadoopOutput.set_output_path`设置输出路径。

`HadoopInput.set_input_paths`方法接收两个参数，第一个参数是Hadoop任务的实例，第二个参数是一个元组（或列表），包含所有输入文件的字符串路径。

`HadoopOutput.set_output_path`方法接收两个参数，第一个参数是Hadoop任务的实例，第二个参数是一个字符串，表示输出文件路径。

文件路径字符串要求使用绝对路径。

```python
hadoop_input = HadoopInput()
hadoop_output = HadoopOutput()
hadoop_input.set_input_paths(job, [input_path])
hadoop_output.set_output_path(job, output_path)
```

这里我们的输入仅有一个，因此输入的文件列表`[input_path]`仅有一个元素。

### 启动Hadoop任务
使用`HadoopInput.start_job`方法启动任务。
任务完成后，使用`HadoopInput.is_successful`方法判断任务是否成功，成功则输出`True`，否则为`False`。
