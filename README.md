# Apache Flink Study


## A look into [FLINK-4391](https://issues.apache.org/jira/browse/FLINK-4391)

### Description

> Many Flink users need to do asynchronous processing driven by data from a DataStream. The classic example would be joining against an external database in order to enrich a stream with extra information.
> It would be nice to add general support for this type of operation in the Flink API. Ideally this could simply take the form of a new operator that manages async operations, keeps so many of them in flight, and then emits results to downstream operators as the async operations complete.

### Disclosure

At this time I have 2 days of experience with Flink, but I am naive enough to speculate on the solution without knowing and probably not even understanding enough Apache Flink.

Also, I had a quick look on the corresponding [pull request](https://github.com/apache/flink/pull/2629/), but not deep enough to fully understand it.

### Ideas

1.  ***If you can avoid it, avoid it!***
    The idea of adding extra asynchronous code on top of a highly asynchronous system gives me chills.
    One of the problems that I see is thread starvation.

2.  ***If you are connecting to an external system using a connection that supports timeout, use that timeout***
    The essential idea is that we can process the input stream, by connection to a remote system in a different branch and join the enriched stream with the original input stream.
    See `org.tupol.flink.timeout.SimpleJoinDemo`.

3.  ***If you want to implement something simple...***
    I have added two ideas for a `RichMapFunction`: `TimeoutMap` and `TimeoutKeyedMap`.
    The basic idea here is that the mapping function will return a `Try[RESULT]` as it wraps the function into a `Future` with a blocking `await`. 
    The simples out of the two is `TimeoutMap`, as there is no need for joining anything and the processing can go. 
    See `org.tupol.flink.timeout.TimeoutDemo` 1 to 4.
    The main problem with these approaches is that an extra thread is created inside each `Task` thread and it would be better if there would be a watchdog per `Task` to deal with the timeout of each thread.

4.  ***If you want to go deeper, the API needs to support a transformation timeout on record and on window***
    *TODO: Study the current pull request better.*
    Another naive thought I had was having something in the Flink runtime `Task`, similar to `taskCancellationTimeout` property, somehow, but probably not the greatest gem.
    

Code for this study is available in the `org.tupol.flink.timeout` package.

| Class             | Description                                                                | Tests              |
| ----------------- | -------------------------------------------------------------------------- | ------------------ |
| `SimpleJoinDemo`  | Using the "natural" timeout and then join the result with original stream  | None               |
| `TimeoutDemo1`    | Like `SimpleJoinDemo` with a coded "timeout"                               | None               |
| `TimeoutDemo2`    | Like `TimeoutDemo1` with a different result type for the `heavyWorkStream` | None               |
| `TimeoutDemo3`    | Sample use of `TimeoutKeyedMap`                                            | `TimeoutDemo3Spec` |
| `TimeoutDemo4`    | Sample use of `TimeoutMap`                                                 | `TimeoutDemo4Spec` |

**Running Notes**

From the project directory, run 
`sbt clean publish-local`
and then 
`flink run -c org.tupol.flink.timeout.TimeoutDemo4 target/scala-2.10/flink-study_2.10-0.1.0.jar`


