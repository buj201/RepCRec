## Design Summary

My implementation of RepCRec has three basic components:
1. A `TransactionManager`, which inherits from the `Parser`.
2. A `SiteManager`, which inherits from the `LockTable`.
3. A `Transaction`, which is managed by the `TransactionManager`.

In addition, communication is achieved across components in a large part through
use of a `RequestResponse` object.

We describe each of the components in individually, then describe their
main interactions and communication.

## `TransactionManager`

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

## `SiteManager`

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

## `Transaction`

There are two types of transactions: `ReadOnlyTransaction` and `ReadWriteTransaction`.

### `ReadOnlyTransaction`

Read only transactions are responsible for maintaining state for _indeterminant_
reads. As described in the detailed documentation, there is an edge case where a
`ReadOnlyTransaction` is trying to read a replicated variable `x`, and every live
site has failed between the time that (i) the read-only transaction began, and
(ii) the site last had a commit where `x` was available. To handle this edge case,
the `ReadOnlyTransaction` is responsible for keeping track of _indeterminant_ reads.

### `ReadWriteTransaction`

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

