from .Variable import Variable
from .LockTable import LockTable
from collections import deque, namedtuple
from copy import deepcopy

Snapshot = namedtuple('Snapshot', ['time', 'snapshot'])

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
    memory : dict mapping variable names (e.g. x1) to Variable object
        Current state of the site
    disk : list of site snapshots
        Each snapshot has a snapshot time, and stores the state of the
        site memory as that time. A new snapshot is added whenver the
        site commits.
    """
    
    N_VARS = 20
    
    def __init__(self,site_number,start_time):
        
        # Set up site clock and status
        self.site_number = site_number
        self.uptime = start_time
        self.alive = True
        
        # All sites have the even numbered variables
        self.memory = {f'x{i}': Variable(i,replicated=True)
                       for i in range(2,self.N_VARS+2,2)}
        
        # If site is in 2,4,...,10, then it als has
        # odd variables x{site_number-1} and x{site_number + 10 - 1}
        if site_number % 2 == 0:
            self.memory.update({
                f'x{i}': Variable(i,replicated=False)
                for i in [(site_number-1),(site_number+9)]
            })
        
        # Initialize lock table
        super().__init__(self.memory,self.site_number)
        
        # Commit an initial snapshot of the DB at time 0
        self.disk = [Snapshot(time=start_time,
                              snapshot={x:deepcopy(v) for x,v in self.memory.items()})]
        
        
    def read_from_disk(self,T,x):
        """ Transaction T reads x from disk. T can only read x
        at this site if:
            - T reads from a commit that occurred before T started
            - x is available in this commit (i.e. this site hadn't
              recovered from failure prior to this commit, with x
              not in the write set of the commmitting transaction)
            - The site has been live since this commit.
        
        Parameters
        ----------
        T : Transaction
            Transaction requesting read on x
        x : Variable name
            Name of variable to read
            
        Returns
        -------
        int, any
            Commit time of snapshot read, value of x in that commit
        """
        
        # Get most recent snapshot on disk by traversing in reversed
        # order, breaking out if we find the most recently committed
        # copy of x at this site
        for snapshot in reversed(self.disk):
            if ((T.start_time > snapshot.time) and 
                (snapshot.snapshot[x].available) and 
                (snapshot.time >= self.uptime)):
                return snapshot.time, snapshot.snapshot[x].read()
        
        # Otherwise the most recent value of x is not available at this
        # site
        return None, None
        
    def read_from_memory(self,x):
        """ Transaction T reads x from memory
        
        Parameters
        ----------
        T : Transaction
            Transaction requesting read on x
        x : Variable name
            Name of variable to read
            
        Returns
        -------
        any
            Current value of x in memory
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
        """ Simulate site failing. Results in:
            - Lock table being erased and waits_for graph being reset
              to the empty graph.
            - Site being set to down (i.e. alive=False)
        """
        # Re-initialize lock table
        super().__init__(self.memory,self.site_number)
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
        """ Commit state to disk as a snapshot.
        
        Parameters
        ----------
        time : int
            Current time (for timestamping snapshot)
        """
        
        to_commit = {x:deepcopy(v) for x,v in self.memory.items()}
        snapshot = Snapshot(
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
        LockTable.remove_T_from_lock_table_and_waitsfor
        """
        # Release all locks and update waits-for graph
        self.remove_T_from_lock_table_and_waitsfor(T)