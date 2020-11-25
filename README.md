# RepCRec

## Running RepCRec from `RepCRec.rpz`

My implementation of RepCRec has been reproducibly packaged using
`reprozip`. Given `RepCRec.rpz`, one can follow the instructions
below to (1) reproducibly run the set of tests I've provided illustrating
my system's execution, and (2) execute instructions for a single
`RepCRec` run from a file.

First, I give instructions for running on a linux machine. Then
I give instructions for running on a Mac. Note that while these instructions give
one way to unpack and run  `RepCRec.rpz`, there are other unpackers,
and other ways to interact with the package. That said, this approach
is simple and uses `reprounzip` effectively.

### Running on Linux

1. **Setup**: Unpack the package with `reprounzip directory setup RepCRec.rpz RepCRec`
2. **Run the provided tests**: Run the provided tests using `reprounzip directory run RepCRec 0`
3. **Execute custom instructions**: To execute custom instructions in a file `new_instructions.txt`,
there are two steps:
    1. **Upload the new instructions**: Upload the new instruction file `new_instructions.txt` using
       the command `reprounzip directory upload RepCRec new_instructions.txt:arg1`
    2. **Run RepCRec using these instructions**: Execute using the command `reprounzip directory run RepCRec 1`
4. **Teardown**: To tear down, simply run `reprounzip directory destroy RepCRec`.

### Running on Mac using Docker

These instructions assume you have already installed `docker`. 

1. **Setup**: Unpack the package with `reprounzip docker setup RepCRec.rpz RepCRec`
2. **Run the provided tests**: Run the provided tests using `reprounzip docker run RepCRec 0`
3. **Execute custom instructions**: To execute custom instructions in a file `new_instructions.txt`,
there are two steps:
    1. **Upload the new instructions**: Upload the new instruction file `new_instructions.txt` using
       the command `reprounzip docker upload RepCRec new_instructions.txt:arg1`
    2. **Run RepCRec using these instructions**: Execute using the command `reprounzip docker run RepCRec 1`
4. **Teardown**: To tear down, simply run `reprounzip docker destroy RepCRec`.

## Design: Components

My implementation of RepCRec has three basic components:
1. A `TransactionManager`, which inherits from the `Parser`.
2. A `SiteManager`, which inherits from the `LockTable`.
3. A `Transaction`, which is managed by the `TransactionManager`.

In addition, communication is achieved across components in a large part through
use of a `RequestResponse` object.

We describe each of the components in individually, then describe their
main interactions and communication.

### `TransactionManager`

The `TransactionManager` is the main module. It is responsible for four main tasks:

1. **Parsing**: The `TransactionManager` accepts input instructions from a 
file, or from standard input. Input instructions
are parsed by methods inherited from `Parser`. This `Parser` parses new
operation requests, returning a `RequestResponse` object.
2. **Routing requests**: `R` and `W` requests from transactions need to 
be routed to sites using the available copies algorithm. The
`TransactionManager` is responsible for routing these requests to live sites.
3. **Managing the request queue**: Due to lock conflicts and site failures,
not all requests can be immediately satisfied. In this case, the
`TransactionManager` is responsible for maintaining a queue of blocked
requests, and attempting to execute these blocked requests in a FIFO manner.
4. **Detecting and correcting deadlocks**: The `TransactionManager` is also
responsible for detecting and correcting deadlocks.

### `SiteManager`

During its initialization, the `TransactionManager` initializes 10 `SiteManager`
objects (corresponding to the 10 sites in the simulation). Each `SiteManager`
is responsible for two basic tasks

1. **LockTable Management**: The `SiteManager` inherits from `LockTable`, and it
is responsible for managing the read and write locks on all variables at the site.
2. **Operation conflicts**:  The `SiteManager` is responsible for maintaining
a subgraph of the overall waits-for graph. This subgraph only includes edges
for operations which (i) were routed to the site, and (ii) are in conflict. Note
each site does **not** maintain the full waits-for graph; that is the responsibility
of the `TransactionManager`.

### `Transaction`

There are two types of transactions: `ReadOnlyTransaction` and `ReadWriteTransaction`.

#### `ReadOnlyTransaction`

Read only transactions are responsible for maintaining state for _indeterminant_
reads. As described in the detailed documentation, there is an edge case where a
`ReadOnlyTransaction` is trying to read a replicated variable `x`, and every live
site has failed between the time that (i) the read-only transaction began, and
(ii) the site last had a commit where `x` was available. To handle this edge case,
the `ReadOnlyTransaction` is responsible for keeping track of _indeterminant_ reads.

#### `ReadWriteTransaction`

Read-write transactions are responsible for:

1. Maintaining the after-image, at each site, for each variable written to by
the transaction.
2. Holding locks, and tracking which locks the transaction needs in order for
its next operation to proceed.

### `RequestResponse`

The `RequestResponse` is an important detail of the intended implimentation. Each
incoming request is parsed by the `Parser` to return a `RequestResponse`. This
`RequestResponse` includes a `callback` -- the function which is called by the
`TransactionManager` when attempting to execute the request. These callbacks
in turn return `RequestResponse` objects, including an indicator of whether
the request was successfuly satisfied, as well as a new callback (if the
request was not satisfied).

## Interaction and Communication

We focus on the interactions involved in a tick of the simulation. The figure
at the end of this section shows the three phases of a tick:

1. Deadlock detection
2. Adding new requests to the end of the `TransactionManager` request queue
3. Attempting to execute each request in the queue.

The interactions involved in step 3 vary depending on the type of request. We
proceed to describe key interactions at a high level of abstraction.

### R/W requests for `ReadWriteTransactions`

1. For `W` requests, the `TransactionManager` checks if the `Transaction`
needs to request additional locks (if there are newly available copies due
to site recover), and brokers those requests if needed.
2. For both `W` and `R` requests, the `Transaction` checks if it is holding all
required locks.
    - If so, it executes and returns `RequestResponse` with `success=True`.
    - Otherwise, it either (i) has the `TransactionManager` broker new lock
    requests (if the `Transaction` is not waiting on a queued lock request), or
    simply waits its turn in any relevant lock queues.

### R requests for `ReadOnlyTransactions`

1. The `TransactionManager` tries to find a live site which has a snapshot with
an available copy of `x`, such that (i) the snapshot was taken before the
`ReadOnlyTransaction` began, and (ii) the site did not die between the time
of this commit and the time the `ReadOnlyTransaction` began.
    - If such a site exists, the `Transaction` reads `x`.
    - Otherwise, the `Transaction` has to attempt to read at all sites in order
      to guarantee it is reading the most recent committed copy of `x`.

### begin, beginRO, dump, fail, recover requests

These requests involve minimal interactions across objects with the exception of
`fail`. `Fail` instructions require the (i) `SiteManager` wipe its lock table,
and (ii) `Transactions` release all locks (and stop waiting on locks) at the
failed site (note this behavior simulates the `TransactionManager` passing a 
message to the `Transactions` instructing them to drop locks on failure).

### end requests

The end requests either lead `ReadWriteTransactions` to commit or to abort.
If they commit, the `SiteManagers` write the `ReadWriteTransactions` after-image,
and snapshot the commit by writing to disk. Regardless of whether or not the
`Transaction` commits or aborts, the `SiteManagers` drop the `Transaction` from
their `LockTable` and waits-for subgraph.
