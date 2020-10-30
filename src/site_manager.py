from .variable import Variable
from .lock_table import LockTable
from collections import deque, namedtuple
from copy import deepcopy
from .request_response import RequestResponse

_Snapshot = namedtuple('_Snapshot', ['time', 'snapshot'])

class SiteManager(LockTable):
    """Class for the site manager. Inherits from LockTable,
    since the LockTable is the core component of the SiteManager.

    Attributes
    ----------
    site_number : int (1-10)
        Site number
    uptime : int
        The beginning of the current up period for the site.
    alive : bool
        True if the site is currently alive
    memory : dict of str : Variable
        Current state of the site; maps variable names to Variables.
    disk : list of _Snapshots
        Each snapshot has a snapshot time, and stores the state of the
        site memory as that time. A new snapshot is added whenver the
        site commits.
    """
    
    N_VARS = 20
    """Total number of variables across sites
    """
    
    def __init__(self,site_number,start_time):
        
        # Set up site clock and status
        self.site_number = site_number
        self.uptime = start_time
        self.alive = True
        
        # All sites have the even numbered variables
        self.memory = {f'x{i}': Variable(i,replicated=True)
                       for i in range(2,self.N_VARS+2,2)}
        
        # If site is in 2,4,...,10, then it also has
        # odd variables x{site_number-1} and x{site_number + 10 - 1}
        if site_number % 2 == 0:
            self.memory.update({
                f'x{i}': Variable(i,replicated=False)
                for i in [(site_number-1),(site_number+9)]
            })
        
        # Initialize lock table
        super().__init__(self.memory)
        
        # Commit an initial snapshot of the DB at time 0
        self.disk = [_Snapshot(time=start_time,
                              snapshot={x:deepcopy(v) for x,v in self.memory.items()})]
        
        
    def try_reading_from_disk(self,request):
        """ Transaction T requests to reads x from disk. T can only read x
        at this site if:
            - There was a commit at this site that occurred before T started
            - x is available in this commit (i.e. this site hadn't
              recovered from failure prior to this commit, with x
              not in the write set of the commmitting transaction)
            - The site has been live since this commit. We check this by
              reference the RO transaction's site_uptimes dict.
        If these conditions are not satisifed, we return a response
        with the most recent available commit on x before T began
        (note every site has at least one such commit). The request
        is not successful, however; instead, the transaction has to
        obtain this indeterminant read at all sites before identifying
        the most recent commit to x.
        
        Parameters
        ----------
        request : RequestResponse
            RequestResponse with a transaction T and variable x.
            
        Returns
        -------
        RequestResponse
            Response to read request, with success indicating the
            transaction was able to read from a committed copy at
            this site. If success is None, then the returned value
            v is potentially not the most recent commit to x across
            all sites.

        See Also
        --------
        :py:meth:`~src.transaction.ReadOnlyTransaction.most_recent_commit_to_x`
        """
        
        # Get most recent snapshot on disk by traversing in reversed
        # order, breaking out if we find the most recently committed
        # copy of x at this site
        T = request.transaction
        x = request.x

        # Case 1 -- if x is not replicated, then we just grab the last
        # snapshot before the RO transaction began
        if not self.memory[x].replicated:
            for snapshot in reversed(self.disk):
                if (T.start_time >= snapshot.time):
                    v = snapshot.snapshot[x].read()
                    return RequestResponse(transaction=T,x=x,v=v,
                                           operation='R',success=True,callback=None)

        else:
            # Case 2a -- then x is replicated. Then we need to check that the
            # snapshot was committed before T started, that x is available in
            # the commit, and that the site hasn't died since the commit.
            # If the site has died, then it's possible the most recent
            # available commit at this site isn't the most recent commit of
            # the variable.
            for snapshot in reversed(self.disk):
                if ((T.start_time >= snapshot.time) and 
                    (snapshot.snapshot[x].available) and 
                    (snapshot.time >= T.site_uptimes[self])):
                    # Then this site has a snapshot with the most recent commited
                    # value of x. Return this value.
                    v = snapshot.snapshot[x].read()
                    return RequestResponse(transaction=T,x=x,v=v,
                                           operation='R',success=True,callback=None)
            
            # Case 2b -- x is replicated, but there is no snapshot
            # from prior to the start of transaction T where x is
            # available, and where the site has not died since the snapshot
            # was made. Since the initial state at the site will have
            # an available copy of x, and be snapshotted when the
            # site is instantiated (at time 0), this condition must fail because
            # of site failure. To handle the (extreme) edge case
            # where all sites have failed between the last available
            # snapshot of x, and the start of transaction T, we return
            # a response with success=None (e.g. hacking ternary logic).
            for snapshot in reversed(self.disk):
                if ((T.start_time >= snapshot.time) and 
                    (snapshot.snapshot[x].available)):
                    # Then this site has a snapshot with an available commited
                    # value of x from before T began -- but, it might
                    # not be the most recent commit (as the site died after the
                    # commit). Return this value along with the commit time,
                    # but indicate indeterminant success with success=None
                    v = snapshot.snapshot[x].read()
                    return RequestResponse(transaction=T,x=x,v=(snapshot.time,v),
                                           operation='R',success=None,
                                           callback=request.callback)
        
        # Raise a runtime error if these conditions don't hold. Every live
        # site should always have a snapshot where every x is available -- 
        # since the initial state of the site has x available
        raise RuntimeError('No available copy of x in any prior snapshot.')
        
    def read_from_memory(self,x):
        """ Read x from memory. Note caller is responsible for
        ensuring that x is available, and the requesting transaction
        is holding the required lock.

        Returns
        -------
        Any
            Value of x at site.
        """
        return self.memory[x].read()
                   
    def write(self,x,v):
        """
        Transaction T writes v to x. Note called during commit
        of a transaction -- so v is the value in the after image
        of site.x for some transaction.

        Parameters
        ----------
        x : Variable name
            Name of variable to read
        v : any
            Value to write
        """
        self.memory[x].write(v)
        
    def fail(self):
        """ Simulate site failing. Results in

            - Lock table being erased and waits_for graph being reset
              to the empty graph.
            - Site being set to down (i.e. alive=False)
        """
        # Re-initialize lock table
        super().__init__(self.memory)
        # Set site down
        self.alive = False
        
    def recover(self,time):
        """ Simulate site recovering. On recovery, we
            - Set all replicated variables to "unavailable"
            - Set site to alive
            - Reset the site's uptime
            
        Parameters
        ----------
        time : int
            Current time (for resetting uptime)
        """
        
        # Set all replicated variables to "unavailable"
        for x in self.memory:
            if self.memory[x].replicated:
                self.memory[x].available = False
                
        # Set site to alive
        self.alive = True
        self.uptime = time
        
    def commit(self,time):
        """ Write memory to disk as a Snapshot. 
        
        Parameters
        ----------
        time : int
            Current time (for timestamping snapshot)
        """
        
        to_commit = {x:deepcopy(v) for x,v in self.memory.items()}
        snapshot = _Snapshot(
            time=time,
            snapshot = to_commit
        )
        self.disk.append(snapshot)
        
    def end(self,T):
        """ Clean up after transaction T is aborted or committed. Either case involves
        releasing all of Ts locks, removing it from the waits-for graph, and 
        reassigning locks as needed.
        
        Parameters
        ----------
        T : ReadWriteTransaction
            Transaction to commit
        
        See Also
        --------
        :py:meth:`~src.lock_table.LockTable.remove_T_from_lock_table_and_waitsfor`
        """
        # Release all locks and update waits-for graph
        self.remove_T_from_lock_table_and_waitsfor(T)