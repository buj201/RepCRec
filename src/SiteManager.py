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
        self.time = start_time
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
        Transaction T reads x from disk.
        
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
        # transaction T started.
        for snapshot in reversed(self.disk):
            if (T.start_time > snapshot.time):
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
            - See variable.write
        """
        self.memory[x].write(v)
        
    def fail(self):
        """
        Simulate site failing. Results in:
            1. Lock table being erased and waits_for graph being reset
               to the empty graph.
            2. Site being set to down
            
        Args:
            None
            
        Returns:
            None
        
        Side effects:
            - Wipe out the lock table and the waits_for graph
            - Set all replicated variables to "unavailable"
        """
        # Re-initialize lock table
        super().__init__(self.memory,self.site_number)
        # Set site down
        self.alive = False
        
    def recover(self):
        """
        Simulate site recovering. On recovery, we
            - Reset 
        """
        # Re-initialize lock table
        super().__init__(self.memory,self.site_number)
        # Load the most recent commit to memory
        self.memory = {k:deepcopy(v) for k,v in self.disk[-1].snapshot.items()}
        
        # Set all replicated variables to "unavailable"
        for x in self.memory:
            if self.memory[x].replicated:
                self.memory[x].available = False
                
        # Set site to alive
        self.alive = True
        
    def commit(self,T):
        """
        Commit state resulting from committed transaction
        by storing to disk as a snapshot.
        Note that we can't just write memory to disk -- as
        memory can include writes from other non-committed transactions.
        To address this, we will walk the lock table. We will
        then undo any writes from transactions besides T which
        currently hold a write lock.
        
        Args:
            T: Transaction
        
        Returns:
            None
        
        Side effects:
            - Adds a new snapshot to memory (labelled with the
              current time).
        """
        
        to_commit = {x:deepcopy(v) for x,v in self.memory.items()}
        
        for x in to_commit:
            WL_holder = self.lock_table[x]['WL']
            if (WL_holder is not None) and (WL_holder != T.name):
                to_commit[x].undo_write()
        
        snapshot = Snapshot(
            time=self.time,
            snapshot = to_commit
        )
        self.disk.append(snapshot)
        
    def abort(self,T):
        """
        Abort transaction T. Aborting involves two steps:
            1. If T holds a WL on variable x, we need to undo the
               write to x.
            2. We need to release all of Ts locks, and remove it
               from the waits-for graph.
        
        Args:
            T: Transaction
        
        Returns:
            None
        
        Side effects:
            - Undo's all of T's writes in memory
            - Releases all of Ts locks
            - Remove T from the waits-for graph (along with incident edges)
        """
        
        # Step 1 -- undo writes in memory
        T_WLs = T.write_locks[self.site_number]
        for x in T_WLs:
            self.memory[x].undo_write()
        
        # Step 2 -- release all locks and update waits-for graph
        self.scrub_transaction_from_table(T)