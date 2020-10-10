import fileinput
from collections import namedtuple,deque
import re

from src.SiteManager import SiteManager
from src.Parser import Parser
from src.Transaction import Transaction,ReadOnlyTransaction

ReadResponse = namedtuple('ReadResponse', ['success', 'value'])
WriteResponse = namedtuple('WriteResponse', ['success'])

class TransactionManager(Parser):
    
    N_SITES = 10
    
    def __init__(self):
        super().__init__()

        self.time = 0
        
        self.sites = {i:SiteManager(i,self.time) for i in range(1,self.N_SITES+1)}
        
        self.transactions = {}
        
        self.instructions = fileinput.input('tests/test2.txt')
        self.buffered_instructions = deque()
        
    def detect_deadlocks_and_abort_youngest_transaction(self):
        """
        Detect cycles in each sites waits-for graph. If cycle(s)
        detected, identify the youngest transaction in the cycle(s)
        and abort this transaction.
        
        Args:
            None
            
        Returns:
            None
            
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
            transactions_in_deadlock = set(self.transactions[x] for x in transactions_in_deadlock)
            ordered_by_age = sorted(transactions_in_deadlock,key=lambda x:x.start_time)
            youngest_transaction = ordered_by_age[-1]

            # Abort this transaction at all sites
            for site_number,site in self.sites.items():
                site.abort(youngest_transaction)

            # Delete this transaction
            print(f'Aborting transaction {youngest_transaction.name} due to deadlock')
            del self.transactions[youngest_transaction.name]
            
            return True
        
        return False

    def tick(self):
        """
        Advance time by one step. When we advance time, we:
            1. Detect deadlocks, and abort the youngest transaction in the cycle.
            2. Re-assign locks.
            3. Attempt to execute 
            
        """
        
        # Step 1: Detect deadlocks and abort transactions
        deadlocked = True
        while deadlocked:
            deadlocked = self.detect_deadlocks_and_abort_youngest_transaction()
        
        # Step 2: Reallocate locks 
        
        self.time += 1
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
        we buffer the instruction and wait.
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
            response = ReadResponse(success=True,value=most_recent_value)
        else:
            self.buffered_instructions.append(f'R({T},{x})')
            response = ReadResponse(success=False,value=None)
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
                    # Then lock is available we can get the lock and read the value
                    RL = site.give_transaction_RL(T,x)
                    T.read_locks[site_number].add(RL.variable)
                    return ReadResponse(success=True,value=site.read_from_memory(x))
                else:
                    # Then it's locked -- but we still could just wait for the lock
                    live_locked_sites.append((site_number,site))
        
        # If no live site had x unlocked, then we need to route to a locked site.'
        if len(live_locked_sites)>0:
            site_number,site = random.choice(live_locked_sites)

            # Then we need to put the transaction in the queue to get a RL on x
            waiting_for = site.RL_available(T,x).transactions
            site.add_transaction_to_lock_queue(T,x,waiting_for,'RL')
        
        # if we put lock request in queue -- or if all sites are down/all copies
        # of x are unavailable -- we need to add this transaction to the end of the
        # buffered instructions
        self.buffered_instructions.append(f'R({T},{x})')
        return ReadResponse(success=False,value=None)
        
    def try_reading(self,T,x):
        """
        Try reading x. Routes request based on whether T is read-only or read-write.
            
        Args:
            T: Transaction
            x: variable
            
        Returns:
            None -- prints to stdout if successful
            
        Side effects:
            See `route_read_write_read` and `route_read_only_read`
            
        """
        # Route read request based on transaction type
        if T.read_only:
            response = self.route_read_only_read(T,x)
        else:
            response = self.route_read_write_read(T,x)
            
        if response.success:
            print(f'{T.name}: {x}={response.value}')
            
        
    def try_writing(self,T,x,v):
        """
        Try writing to all live servers. If a WL is available at all servers,
        then we obtain the WLs. Otherwise we need to wait in queue
            
        Args:
            T: Transaction
            x: variable
            
        """
        all_write_locks_available = True
        for site_number,site in self.sites.items():
            # Ignore sites that don't contain x or are down
            if (site.alive) and (x in site.memory) and (all_write_locks_available):
                if not site.WL_available(T,x).response:
                    all_write_locks_available = False
                    
        # If all write locks are available, then we obtain locks and write
        if all_write_locks_available:
            for site_number,site in self.sites.items():
                # Ignore sites that don't contain x or are down
                if (site.alive) and (x in site.memory):
                    WL = site.give_transaction_WL(T,x)
                    T.write_locks[site_number].add(WL.variable)
                    site.write(x,v)
            return WriteResponse(success=True)
        else:
            # Otherwise we can't get some lock -- so we simply add the
            # lock request to the queue everywhere.
            for site_number,site in self.sites.items():
                # Ignore sites that don't contain x or are down
                if (site.alive) and (x in site.memory):
                    available, waiting_for = site.WL_available(T,x)
                    site.add_transaction_to_lock_queue(T,x,waiting_for,'WL')
            return WriteResponse(success=False)
        
        