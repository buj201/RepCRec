from .Variable import Variable
from .LockTable import LockTable
from collections import deque, namedtuple
from copy import deepcopy

Snapshot = namedtuple('Snapshot', ['time', 'snapshot'])

class SiteManager(LockTable):
    
    N_VARS = 20
    
    def __init__(self,site_number,start_time):
        
        # Set up site clock and status
        self.site_number = site_number
        self.uptime = start_time
        self.alive = True
        
        # All sites have the even numbered variables
        self.memory = {f'x{i}': Variable(i,replicated=True)
                       for i in range(2,self.N_VARS+2,2)}
        
        # If site is in 2,4,...,10, also have odd variables x{site_number-1} and x{site_number + 10 - 1}
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
        """
        Transaction T reads x from disk, by reading the most recent
        snapshot committed before T began. Note our implementation
        allows commits where a variable is not available -- so we need
        to ensure we return a snapshot where x is available
        
        Args:
            T: Transaction
            x: variable
            
        Returns:
            Commit time of snapshot read from disk, and value read by T
            
        Side effects:
            None
        """
        
        # Get most recent snapshot on disk by traversing in reversed
        # order, breaking out when we first find a commit from before the
        # transaction T started where x is available
        for snapshot in reversed(self.disk):
            if ((T.start_time > snapshot.time) and (snapshot.snapshot[x].available)):
                return snapshot.time, snapshot.snapshot[x].read()
        
    def read_from_memory(self,x):
        """
        Transaction T reads x from memory
        
        Args:
            T: Transaction
            x: variable
            
        Returns:
            value read by T
            
        Side effects:
            None
        """
        return self.memory[x].read()
        
            
    def write(self,x,v):
        """
        Transaction T writes v to x.
        
            T: Transaction
            x: variable
            v: value written to x
            
        Returns:
            None
            
        Side effects:
            None
        """
        self.memory[x].write(v)
        
    def fail(self):
        """
        Simulate site failing. Results in:
            - Lock table being erased and waits_for graph being reset
              to the empty graph.
            - Site being set to down (i.e. alive=False)
            
        Args:
            None
            
        Returns:
            None
        
        Side effects:
            None
        """
        # Re-initialize lock table
        super().__init__(self.memory,self.site_number)
        # Set site down
        self.alive = False
        
    def recover(self,time):
        """
        Simulate site recovering. On recovery, we
            - Set all replicated variables to "unavailable"
            - Set site to alive
            - Reset the site's uptime
            
        Args:
            time: current time
            
        Returns:
            None
            
        Side-effects:
            None
        """
        
        # Set all replicated variables to "unavailable"
        for x in self.memory:
            if self.memory[x].replicated:
                self.memory[x].available = False
                
        # Set site to alive
        self.alive = True
        self.uptime = time
        
    def commit(self,time):
        """
        Commit state to disk as a snapshot.
        
        Args:
            None
        
        Returns:
            None
        
        Side effects:
            - Adds a new snapshot to memory (labelled with the
              current time).
        """
        
        to_commit = {x:deepcopy(v) for x,v in self.memory.items()}
        snapshot = Snapshot(
            time=time,
            snapshot = to_commit
        )
        self.disk.append(snapshot)
        
    def end(self,T):
        """
        Clean up after transaction T is aborted or committed. Either case involves
        releasing all of Ts locks, removing it from the waits-for graph, and 
        reassigning locks as needed.
        
        Args:
            T: Transaction
        
        Returns:
            None
        
        Side effects:
            - Releases all of Ts locks
            - Gives locks to waiters
            - Remove T from the waits-for graph (along with incident edges)
        """
        # Release all locks and update waits-for graph
        self.remove_T_from_lock_table_and_waitsfor(T)