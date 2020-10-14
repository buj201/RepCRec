import fileinput
from collections import namedtuple,deque
import re

from src.SiteManager import SiteManager
from src.Parser import Parser
from src.Transaction import ReadWriteTransaction,ReadOnlyTransaction
from src.nts import WriteOp,ReadOp,ReadResponse,WriteResponse

class TransactionManager(Parser):
    """
    The transaction manager is response for managing each transaction
    and the incoming instructions.
    
    Attributes
    ----------
        time : int
            current time (number of ticks)
        sites : dict
            Maps site numbers (1-10) to SiteManagers
        transactions : dict
            Maps transaction names (ex: T1) to Transactions
        instructions : fileinput input
            Input file, or stdin, from which instructions are read
        transaction_queue : deque
            A queue storing (transaction, callback) to execute for blocked functions
            
    Methods
    -------
        
        
    """
    N_SITES = 10
    
    def __init__(self,test):
        super().__init__()
        self.time = 0
        self.sites = {i:SiteManager(i,self.time) for i in range(1,self.N_SITES+1)}
        self.transactions = {}
        self.instructions = fileinput.input(test)
        
        # The transaction queue
        self.transaction_queue = deque()
        
    def identify_youngest_transaction_in_cycle(self):
        """
        Detect cycles in each sites waits-for graph. If cycle(s)
        detected, identify the youngest transaction in the cycle(s).
        
        Args:
            None
            
        Returns:
            None if no cycles, else the name of the youngest transaction in a cycle.
            
        Side effects:
            If there is a deadlock at any site, detect and kill the
            youngest transaction in the cycle.
        """
        transactions_in_deadlock = set()
        for site_number, site in self.sites.items():
            has_cycles = site.waits_for_has_cycles()
            if has_cycles.response:
                transactions_in_deadlock = transactions_in_deadlock.union(has_cycles.transactions)
            
        if len(transactions_in_deadlock)>0:
            ordered_by_age = sorted(transactions_in_deadlock, key=lambda x: x.start_time)
            youngest_transaction = ordered_by_age[-1]
            return youngest_transaction
    
        else:
            return None
        
    def sweep_transaction_queue(self):
        """
        Sweep through transaction queue in order, calling callbacks. 
        """
        new_queue = deque()
        while len(self.transaction_queue) > 0:
            next_transaction = self.transaction_queue.popleft()
            response = next_transaction.callback()
            if not response.success:
                new_queue.append(next_transaction)
        self.transaction_queue = new_queue
        
        
    def abort(self,T,msg):
        """
        Abort transaction T at all sites.
        
        Args:
            T: transaction
            msg: reason for abort
            
        Returns:
            None
            
        Side effects:
            Abort transaction T at all sites, move
            this transaction from the TM's supervision,
            and remove from the transaction queue (if blocked)
        """
        # End this transaction at all sites
        for site_number,site in self.sites.items():
            site.end(T)

        # Delete this transaction
        del self.transactions[T.name]
        
        # Update the transaction queue
        new_q = deque()
        while (len(self.transaction_queue)>0):
            next_t = self.transaction_queue.popleft()
            if not next_t.transaction == T:
                new_q.append(next_t)
        self.transaction_queue = new_q
        print(f'Aborting transaction {T.name} due to {msg}')

    def commit(self,T):
        """
        Commit a read-write transaction T.
        
        Args:
            None
            
        Returns:
            None
            
        Side effects (for read-write transactions):
            - Writes all writes in T.after_image to corresponding sites
            - Removes T from the waits-for graph and lock table
        """
        for site in T.after_image:
            # Write to memory
            for x,v in T.after_image[site].items():
                site.write(x,v)
            # Then commit to disk
            site.commit(self.time)
            site.end(T)
        print(f"{T.name} commits")
            
    def tick(self):
        """
        Advance time by one step. When we advance time, we:
            1. Detect deadlocks, and abort the youngest transaction in the cycle.
            2. Attempt to execute callbacks in queue
            3. Attempt to execute next instruction
            
        """
        
        # Step 0: Increment time
        self.time += 1
        
        # Step 1: Detect deadlocks and abort transactions
        deadlocked = True
        while deadlocked:
            youngest_transaction_in_cycle = self.identify_youngest_transaction_in_cycle()
            if youngest_transaction_in_cycle is None:
                deadlocked = False
            else:
                self.abort(youngest_transaction_in_cycle,'deadlock')
        
        # Step 2: Try executing callbacks in queue
        self.sweep_transaction_queue()
        
        # Step 3: Try executing callbacks in queue
        try:
            next_instruction = self.instructions.__next__()
            self.p_line(next_instruction)
        except StopIteration:
            print('Done')
            
        
    def route_read_only_read(self,T,x):
        """
        Route a read-only read request from transaction T for variable x.
        
        Need to find the server satisfying:
            - Server is alive
            - Server has most recently commited version of the DB, subject
              to contraint that the commit was before T's start_time
        If this server exists, then we return the read value. Otherwise
        this transaction is blocked and needs to wait for a server to
        come back online.
        
        Args:
            T: Read-only transaction requesting a read on x
            x: variable to read
            
        Returns:
            ReadResponse
        """
        most_recent_commit_time = -1
        most_recent_value = None
        for site_number,site in self.sites.items():
            # Ignore sites that don't contain x or are down.
            if (x in site.memory) and (site.alive):
                commit_time, commit_value = site.read_from_disk(T,x)
                if commit_time > most_recent_commit_time:
                    most_recent_commit_time = commit_time
                    most_recent_value = commit_value
        
        if most_recent_value is not None:
            # Then found an alive server, with an available committed copy of x,
            # where the commit time is before the transaction start time
            response = ReadResponse(success=True,value=most_recent_value,callback=None,transaction=T)
        else:
            # Didn't find such a server -- so need to try to find one at next tick
            response = ReadResponse(success=False,value=None,
                                    callback=lambda: self.route_read_only_read(T,x),
                                    transaction=T)
        return response
        
    def route_read_write_read(self,T,x):
        """
        Route a read-write read request from transaction T for variable x.
        
        Need to find the server satisfying:
            - Server is alive
            - Variable is available (e.g. not unavailable due to failure)
            - Variable x is not locked
            
        If no servers satisfy these conditions, then we find a server:
            - Server is alive
            - Variable is available (e.g. not unavailable due to failure)
            - Request lock on x and wait
            
        Args:
            T: Transaction
            x: variable
            
        Returns:
        """
        # We'll store a list of locked, live sites containing x in case
        # we need to route read to one of these sites
        live_locked_sites = []
        
        for site_number,site in self.sites.items():
            # Ignore sites that don't contain x or are down
            if (site.alive) and (x in site.memory) and (site.memory[x].available):
                if site.RL_available(T,x).response:
                    # Then lock is available so T can get the lock ...
                    RL = site.give_transaction_RL(T,x)
                    # ... and read the value
                    return ReadResponse(success=True,
                                        value=T.read(site,x),
                                        callback=None,
                                        transaction=T)
                else:
                    # Then it's locked -- but we still could just wait for the lock
                    live_locked_sites.append((site_number,site))
        
        # If no live site had x unlocked and available, then we need to route to a locked site.
        if len(live_locked_sites)>0:
            site_number,site = random.choice(live_locked_sites)

            # Then we need to put the transaction in the queue to get a RL on x
            waiting_for = site.RL_available(T,x).transactions
            site.add_transaction_to_lock_queue(T,x,waiting_for,'RL')
            
            # Updated transactions next_op
            T.next_op = ReadOp(x)
            T.locks_needed[site].add(x)
        
            # The callback is then T.try_again -- T just needs to waits
            # to get this read lock at this site
            return ReadResponse(success=False,value=None,callback=T.try_again,transaction=T)
        
        # If there are no live sites with x available, then we need to give up and
        # retry later
        return ReadResponse(success=False,value=None,
                            callback=lambda: self.route_read_write_read(T,x),
                            transaction=T)
    
        
    def try_reading(self,T,x):
        """
        Try reading x. Routes request based on whether T is read-only or read-write.
            
        Args:
            T: Transaction
            x: variable
            
        Returns:
            None -- prints to x = to stdout if successful
            
        Side effects:
            See `route_read_write_read` and `route_read_only_read`
            
        """
        # Route read request based on transaction type
        if T.read_only:
            response = self.route_read_only_read(T,x)
        else:
            response = self.route_read_write_read(T,x)
                                                 
        if not response.success:
            self.transaction_queue.append(response)
        else:
            print(f'{T.name}: {x}={response.value}')
            
        
    def try_writing(self,T,x,v):
        """
        Try writing to all live servers. If a WL is available at all servers,
        then we obtain the WLs and write. Otherwise we need to wait in queue.
        Then there are two subcases -- either we've requested locks, and
        are waiting for those locks -- or all sites with x were down.
        
            
        Args:
            T: Transaction
            x: variable
            v: value to write to x
            
        Returns:
            WriteResponse with bool indicating whether all WLs successfully obtained
            
        Side effects:
            - If all sites provide WL, then write x = v to all live sites with x.
            - Otherwise, 
            
        """
        at_least_one_available_site = False
        all_write_locks_available = True
        for site_number,site in self.sites.items():
            # Ignore sites that don't contain x or are down
            if (site.alive) and (x in site.memory):
                at_least_one_available_site = True
                if not site.WL_available(T,x).response:
                    all_write_locks_available = False
                    
        # If all write locks are available, then we obtain locks and write
        if all_write_locks_available and at_least_one_available_site:
            for site_number,site in self.sites.items():
                # Ignore sites that don't contain x or are down
                if (site.alive) and (x in site.memory):
                    WL = site.give_transaction_WL(T,x)
                    T.write(site,x,v)
            return WriteResponse(success=True,callback=None,transaction=T)
        elif not at_least_one_available_site:
            # If no available sites then we just need to give up and try again later
            response = WriteResponse(success=False,callback=lambda: try_writing(T,x,v),transaction=T)
            self.transaction_queue.append(response)
        
        else:
            # Otherwise there are available sites -- but we can't get all of the locks
            # so we simply add the lock request to the queue at each live site
            
            # Updated transactions next_op
            T.next_op = WriteOp(x,v)
                    
            for site_number,site in self.sites.items():
                # Ignore sites that don't contain x or are down
                if (site.alive) and (x in site.memory):
                    available, waiting_for = site.WL_available(T,x)
                    site.add_transaction_to_lock_queue(T,x,waiting_for,'WL')
                    T.locks_needed[site].add(x)
        
            # The callback is then T.try_again -- T just needs to waits
            # to get this read lock at this site
            response = WriteResponse(success=False,callback=T.try_again,transaction=T)
            self.transaction_queue.append(response)
        
        