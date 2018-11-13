# Simple Sagas

## Caveat

***Please note:** This repo is **experimental***.

The following could happen to this code:
* Nothing
* It could be refactored, rewritten or modified extensively
* It could disappear
* It could be ported to Java, Kotlin or something else

Now that that's out the way...

## Introduction

A Saga is a sequence of operations in a distributed environment that spans multiple transactional boundaries. 

A saga generally represents a single complex logical business transaction that consists of several steps or sub-transactions that need to be executed separately.

If one of the operations fails it may be necessary to execute compensating actions against those actions that have already been successfully executed.

Simple Sagas is a simple, flexible and resilient mechanism for executing sagas, where all inter-process communication is handled via Kafka. 
It takes full advantage of the robustness and fault-tolerance provided by Kafka and Kafka Streams.

It also integrates natively with the [Simple Sourcing](https://http://simplesource.io/) event sourcing framework.

It also provides a simple and lightweight way of introducing operations with external effects (outside of Kafka) into the Kafka streaming world, 
without the extensive overhead of including and interfacing with an additional framework such as [Akka Streams via Alpakka](https://doc.akka.io/docs/akka-stream-kafka/current/).

## Quick Start

At the project folder, in separate terminal windows:

1. Start kafka stack
    ```bash
    docker-compose up
    ```
1. Start the command processor, the action processor and the saga coordinator:
    
    ```bash
    sbt "user/runMain all.App"
    ```

1. Run the client app to submit some saga requests
    ```bash
    sbt "user/runMain client.App"
    ```
    
The Kafka topics are created as required.
    
If it all runs correctly, you should see some console output that ends with this sort of thing:
```text
09:02:24.725 [saga-...] INFO  SagaStream - stateTransitionsActionResponse: 1b230f=SagaActionStatusChanged(1b230fc9-fa9b-40f7-8007-01e5831f4d93,a2ad32f5-54b0-43b4-bb24-49678e508c56,Completed)
09:02:24.963 [saga-...] INFO  SagaStream - sagaState: 1b230f=InProgress=>5-(e06ac7,Completed)-(719a58,Completed)-(a2ad32,Completed)
09:02:25.222 [saga-...] INFO  SagaStream - sagaState: 1b230f=Completed=>6-(e06ac7,Completed)-(719a58,Completed)-(a2ad32,Completed)
```

It is also possible to run the command processor, the action processor 
and the saga coordinator a separate processes.
In a production environment this would probably be the recommended practice.

1. Simple sourcing command processor
    ```bash
    sbt "user/runMain command.App"
    ```

1. Action processor
    ```bash
    sbt "user/runMain action.App"
    ```

1. Saga coordinator
    ```bash
    sbt "user/runMain saga.App"
    ```
 
There are also scripts to follow the progress of the Saga. e.g.:
```bash
scripts/dump_topic.sh saga_command_account-aggregate
scripts/dump_topic.sh saga_coordinator_saga-saga_state --from-beginning
scripts/dump_topic.sh saga_coordinator_saga-saga_state_transition --from-beginning
```

## Components

There are quite a few moving parts. Because these are mainly pure KStreams apps, there are a lot of deployment options.
Multiple KStream apps can be run in the same process, or they can be split each into their own process. 

The components are:
* A command processor (typically Simple Sourcing App)
* A saga coordinator
* Action processor (an adaptor between the saga coordinator and the command processor)
* A simprle example implementation of the above components
* A saga requester that sends saga requests to the saga request topic

All communication between each of these processes is via Kafka topics.

### Command Processor

Command processor refers in general to the process that executes the single units of work represented by actions.

#### Simple Sourcing Command Processor

A standard Simple Sourcing application. The example here uses the user and account aggregates. 

Note that the Simple Sourcing `CommandAPI` is not exposed. All communication with the app is via messages (command request and response topics).

### Action Processor

An adaptor layer that takes saga-specific actions and translates them command requests.
It turns the command response from the command processor into an action response.

This action processor is idempotent. If an action request with the same action ID (command ID) is resubmitted, 
it will re-emit the action response, but will not re-execute the command. 
We don't currently make use of this feature, as retries are not currently implemented.

All that's required for an action processor listen to the action request topic, execute an action (potentially with effects), 
and publish to the action response topic when complete.

An action processor application is created with one or more action processor handlers. 
Several handlers may be created for processing Simple Sourcing actions, one for each aggregate.
Other action handlers may be added to process other types of actions.

Each action request has an `actionType` parameter. This determines which action processor handles the action. 
It's important that only one action processor handles an action.

#### Simple Sourcing Action Processor

This is the base implementation of an action processor. It turns saga action requests into Simple Sourcing action requests.
It gets the latest sequence Id from the the stream of command responses for the aggregate. 
It then forwards the Simple Sourcing command response back to the sagas coordinator. 

#### Async Action Processor

This is very experimental. It works as follows:
* As with any action processor, it listens for requests on the action request topic
* When an appropriate request arrives, it executes an arbitrary asynchronous function (that returns a future)
* when the future completes, the result status (success, or failure with error) is published in the action response topic
* Optionally, the full return value is published in an arbitrary result topic (in a transaction with the above). 
   
  The client has the opportunity to perform a synchronous conversion of the data before it is persisted.
* This is also an idempotent operation - if a successful request is resubmitted, the response is republished
in the action response topic, but the result value is not republished in the result topic.

  *Of course executed operation needs to be idempotent as well - it has no way of handling the case where the external process completes successfully, but the response never arrives*

This implementation uses the Kafka consumer / producer API as well as the streams API.

This process enables the sagas with long running processes that are executed asynchronously and that have external effects.

It does not currently have [back-pressure](https://www.reactivemanifesto.org/glossary#Back-Pressure) support. However it should
be reasonably straightforward to add this in future.

#### Http Action Processor

This is a thin layer over the async action processor. 
This wraps the async processor with an http client interface.

The choice of http client implementation is left to the user. A simple example using [Requests-Scala](https://github.com/lihaoyi/requests-scala) is provided. 

*This code is experimental.*

### Saga Coordinator

Accepts a dependency graph of sagas actions. It then executes these actions in the order specified by
the dependency graph. An action is executed once its dependencies have successfully executed. 
Actions that are not dependent on one another can be executed in parallel.

Action execution involves submitting to the action request topic and waiting for it to finish by 
listening to the action response topic.

The result of action execution leads to a saga state transition. 
When this happens the next action(s) can be submitted, or if all actions have completed, 
finishing the saga and publishing to the saga response topic.

If any of the actions fail, the actions that are already completed are undone, if a undo action is defined.
Actions are undone in the reverse order that.

### Saga DSL

A simple DSL is provided to simplify creating sagas, loosely based on the [Akka Streams Graph DSL](https://doc.akka.io/docs/akka/2.5/stream/stream-graphs.html)

1. Create a builder:
    ```scala
    import saga.dsl._
   
    val builder = SagaBuilder[A]()
    ```

2. Add some actions:

    ```scala
    val a = builder.addAction(...)
    val b = builder.addAction(...)
    val c = builder.addAction(...)
    val d = builder.addAction(...)
    val e = builder.addAction(...)
    ```

3. Defined dependencies between actions:

    **Examples:**
    
    Execute `a`, then `b`, then `c`:
    ```scala
    a ~> b ~> c
    ```
    
    ----------
    
    Execute `a`, then `b`, `c` and `d` in parallel, then `e`:
    ```scala
    a ~> b ~> e
    a ~> c ~> e
    a ~> d ~> e
    ```
    This can also be expressed as:
    ```scala
    a ~> inParallel(b, c, d) ~> e
    a ~> List(b, c, d).inParallel ~> e
    ```
    
    ----------
    
    Execute `a`, then `b`, `c` and `d` in series, then `e`. The following are equivalent:

    ```scala
    a ~> b ~> c ~> d ~> e
    inSeries(a, b, c) ~> d ~> e
    inSeries(a, b, c, d, e)
    a ~> inSeries(b, c, d) ~> e
    a ~> List(b, c, d).inSeries ~> e
    ```
    This is useful when we have a sequence of actions of length only known at runtime.
    
    Note that the `~>` operator is associative, so 
    `(a ~> b) ~> c` is equivalent to `a ~> (b ~> c)`.
   
4. Build the immutable sagas graph:
   
   ```scala
   val eitherSaga: Either[SagaError, Saga[A]] = builder.build()
   ```

### Client API

Currently the only way to request a sagas and validate its response is via the 
sagas request and response Kafka topics respectively.

The intention is to build a client API that allows a user to submit a sagas request and query its state, 
without having to interact directly with Kafka 
(though this will still remain an option). 

### User Client Applications

This consists of the following example streams apps:

* Example command processor
* Example action processors - a "Sourcing" that interacts with Simple Sourcing, and a "Async" processor that executes some basic arbitrary function.
* A sagas manager that is aware of these action processors
* A simple Kafka producer client to submit sagas

## PoC Scope

* There is currently no support for retries and timeouts. If the command processor fails to return a result, or the 
action processor fails to forward this result to the saga manager, the saga will simply not complete.

    It is slightly tricky to implement this feature, but should become quite straightforward once the following issue is implemented in Kafka Streams:
    
    https://issues.apache.org/jira/browse/KAFKA-6556

## Design and Implementation Notes

1. This app is written in Scala, mainly for the convenience of free Serde derivation for Json serialization.
    It's definitely not pure FP. 
    The expectation is pretty strong that it's going to be ported to Java.
    For this reason the development has shied away from more complex Scala language features and frameworks.
    
1. The distinction between actions and commands is probably not that clear. I'm open to discussion on naming if things help.
    - An action is shorthand for a saga action. Actions are saga aware. 
      Action requests are published in the action request topic, and keyed by saga ID.
    - Actions represent a node in the saga dependency graph. When in undo mode, the arrows of the dependency graph are effectively reversed, but the actions are still the same.
    - For this reason there are the following IDs associated with an action:
        + saga ID
        + action ID
        + command ID
      When executing the undo operation for an action, the saga ID and the action ID will be the same, but the command ID will be different.  
      Idempotence is with respect to the command ID. 
      So if we want to retry submitting a failed action (e.g. optimistic locking fails with an invalid sequence number), we need to modify the command ID.
    - Commands are effectively action executions. So an action has associated with it a command, and optionally, and undo command.

1. Saga commands must have a uniform representation for all action / command types in the sagas. 
    It's a hard to offer anything better in a way that can be ported to Java. It could be done in Scala with dependent types.
    
    This uniform type is the type `A` is the sagas and action APIs. 
    In the example, this is `Json`. In production code with Avro, something like `Record` may be suitable.
    
    The action processor needs to know how to turn an `A` into a `C` (the Simple Sourcing command type) and a `K` (the aggregate key).
    
    To do this we introduce an intermediate decoded type `D`, and a decode operation `A => Either[Throwable, D]`.
    
    The type `D` should be rich enough that the operations `D => K` and `D => C` never throw.


1. The saga implementation follows a state transition model. 
    This may not be the most efficient, but I think it is pretty clean. It makes the sagas execution fully event sourced.
    
    The alternative may be to apply the state changes directly. This may be more efficient as it cuts out an extra step.
    I think it's nice to have the full audit trail of saga state changes.
    
    We can probably apply log compaction on the saga `state` topic, and treate the `state_transition` as an event stream.
1. The actions requests and responses are saved in a single request and response topic for all actions.
    Action processors direct requests to different requests handlers based on the `actionRequest` parameter.
    This is not ideal as there is nothing stopping two action request handlers sharing the same `actionType`. 
    This would result in an action request being processed twice, which result in undetermined behaviour.
    
    A more robust configuration would be for each action request type to be published to a separate topic.
    This results in a lot more topics, and a more complex topology for the sagas app.
    
    As it is, care must be taken not to reuse the `actionType` parameter across different action processor handlers. 


## Future directions

The saga state transition model is quite flexible. Currently we're using it for:
* Initialising the saga
* Changing the status of an action based on saga action responses
* Switching the saga into failure mode if an action fails
* Concluding the saga

At the moment the sagas definition is static. It should be possible to extend the design to accommodate saga transitions that modify the action definitions and even the saga dependency graph.

Because the state transitions are published in a streams, it could be possible to introduce custom saga state transitions processes that
deployed separately from the sagas coordinator app.

Some specific use cases:

### Dynamic undo actions

Currently only statically defined undo/compensation processes are supported. 
It should be straightforward to support dynamically defined undo, but this may be a specialisation of a more general capability that could be added.

It also requires the support of the command processing app.

### Conditional Sagas

Take the following auction use case:

The auction closes with a sequence of bids. 
The highest bid wins, but if this fails to settle because the user has insufficient funds,
it should then pass through to the next highest bid, and successively repeat this until successfully settled.

To support this use case requires conditional saga logic. 
 