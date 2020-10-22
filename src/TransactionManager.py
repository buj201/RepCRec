import fileinput
from collections import namedtuple,deque
import re
import networkx as nx
import random

from src.SiteManager import SiteManager
from src.Parser import Parser
from src.Transaction import ReadWriteTransaction,ReadOnlyTransaction
from src.request_response import Request

class TransactionManager(Parser):
    """
    The transaction manager is responsible for translates read and write
    requests on variables to read and write re-quests on copies using
    the available copy algorithm. It both parses and routes incoming
    requests, and manages a queue of blocked requests.
    
    Attributes
    ----------
    time : int
        Current time (number of ticks)
    sites : dict
        Maps site numbers (1-10) to SiteManagers
    transactions : dict 
        Maps transaction names (ex: T1) to Transactions
    instructions : fileinput input
        Input file, or stdin, from which instructions are read
    request_queue : deque
        A queue storing Requests, with callbacks to execute for operations
        that were blocked by lock conflicts or failures
    """
    N_SITES = 10
    
    def __init__(self,test):
        super().__init__()
        self.time = 0
        self.sites = {i:SiteManager(i,self.time) for i in range(1,self.N_SITES+1)}
        self.transactions = {}
        self.instructions = fileinput.input(test)
        self.request_queue = deque()
    
    def main(self):
        """Main function that reads entire input command.
        """
        done = False
        while not done:
            done = self.tick()

    def backlogged_requests(self):
        """Has backlogged requests
        """
        return len(self.request_queue)>0

    def compose_waits_for_graphs(self):
        """Composes the waits_for graphs from each site.
            
        Returns
        -------
        digraph
            Composed waits for graph from each site
        """

        composed = nx.algorithms.operators.all.compose_all(
            [s.waits_for for s in self.sites.values()]
        )
        return composed
    
    def waits_for_cycle(self,composed_waits_for):
        """
        Check for cycles in the composed waits_for graph.
        
        Parameters
        ----------
        composed_waits_for : digraph
            Composition of all of the waits-for graphs
            
        Returns
        -------
        set
            Set of transactions in the detected cycle in the waits for graph.
            Empty set if no cycles in the composed waits for graph.
        """
        try:
            cycle = nx.algorithms.cycles.find_cycle(composed_waits_for)
            nodes_in_cycle = set()
            for e in cycle:
                for task in e:
                    nodes_in_cycle.add(task)
            return nodes_in_cycle
        except nx.NetworkXNoCycle:
            return None


    def identify_youngest_transaction_in_cycle(self):
        """Detect cycles in waits-for graph. If cycle(s)
        detected, identify the youngest transaction in the cycle(s).
        
        Returns
        ----------
        None or str:
            If no cycles, return None. Else return name of the youngest
            transaction in a cycle (ex: T1).
        """
        composed_waits_for = self.compose_waits_for_graphs()
        cycle = self.waits_for_cycle(composed_waits_for)

        if cycle is not None:
            ordered_by_age = sorted(cycle, key=lambda x: x.start_time)
            youngest_transaction = ordered_by_age[-1]
            return youngest_transaction
        else:
            return None

    def unlock_deadlocks(self):
        """Detect deadlocks, and "unlock" deadlocks by aborting youngest
        transaction in waits-for graph.
        """
        deadlocked = True
        while deadlocked:
            youngest_transaction_in_cycle = self.identify_youngest_transaction_in_cycle()
            if youngest_transaction_in_cycle is None:
                deadlocked = False
            else:
                self.abort(youngest_transaction_in_cycle,'deadlock')

    def refresh_write_lock_requests(self,next_request):
        """Check if the write transaction needs to request write locks at any
        sites that have come back on line. If so, request said locks.

        Parameters
        ----------
        next_request : Request, for a "W" operation

        Returns
        -------
        None

        Side effects
        ------------
        Either adds the transaction to the lock queue, or gives the transaction
        the write lock, at each site which is (i) alive, and (ii) the
        transaction is not holding or waiting for a lock.
        """
        T = next_request.transaction
        x = next_request.x
        for site_number, site in self.sites.items():
            if (site.alive) and (x in site.memory):
                # Check that the transaction knows it needs the lock on site.x
                if x not in T.locks_needed[site]:
                    # If not, then we need to try to give T the lock, or
                    # add it to the queue, and update T's lock_needed dict
                    T.locks_needed[site].add(x)
                    lock_available, waiting_for = site.WL_available(T,x)
                    if lock_available:
                        site.give_transaction_WL(T,x)
                    else:
                        site.add_transaction_to_lock_queue(T,x,waiting_for,'WL')
                        
    def get_dead_sites(self):
        """Check site status, and return list of sites that are dead.

        Parameters
        ----------
        None

        Returns
        -------
        List of SiteManagers
            List of dead sites
        """
        dead_sites = []
        for site in self.sites.values():
            if not site.alive:
                dead_sites.append(site)
        return dead_sites

    def have_transactions_drop_locks_at_dead_sites(self):
        """Have each transaction drop the locks its holding (or waiting on)
        at dead sites.

        Side effects
        ------------
          - Each transaction drops locks (and stops waiting on locks) from dead sites
        """

        # Check site status
        dead_sites = self.get_dead_sites()
        # Tell transactions to remove the locks they're holding at these sites
        for T in self.transactions.values():
            T.drop_locks_at_dead_sites(dead_sites)

    def execute_next_request(self,next_request):
        """Execute the next blocked request.

        Parameters
        ----------
        next_request : Request
            A Request object, with a Transaction.try_again callback, from
            the request queue.

        Returns
        -------
        Request
            A new Request object. If the Transaction is waiting on locks,
            then it calls it's try_again callback.
            If it isn't waiting on any locks, then it
            gives up and goes back to the TM to be rerouted.
        """

        # If the operation is a write, refresh the write lock requests
        if next_request.operation == 'W':
            self.refresh_write_lock_requests(next_request)

        # Check if the transaction is blocked due to lock conflicts
        lock_blocked = next_request.transaction.waiting_on_locks()
        
        # If lock blocked, we simply call the try_again callback
        if lock_blocked:
            response = next_request.callback(self.time)
        else:
            # Otherwise, TM needs to try rerouting the request to live sites.
            if next_request.operation == 'W':
                new_callback = lambda time: self.try_writing(T,next_request.x,next_request.v,time)
            elif next_request.operation == 'R':
                new_callback = lambda time: self.route_read_write_read(T,next_request.x)
            # Call new callback function to get a new Request
            response = new_callback(self.time)

        return response

    def sweep_request_queue(self):
        """Sweep through request queue in order, calling callbacks. 
        Note the request queue is a queue of blocked operations,
        where the requested operation is blocked due to either lock
        conflicts or due to failure.
        
        Side effects
        ------------
        - Deletes dead sites from each transaction lock sets.
        - If the operation is a write, check that the operation is waiting
          for the write lock at all live sites with the variable. If not,
          request locks as needed so the write operation will write to all
          available copies.
        """
        new_queue = deque()

        self.have_transactions_drop_locks_at_dead_sites()

        while self.backlogged_requests():
            next_request = self.request_queue.popleft()
            # Try execution
            response = self.execute_next_request(next_request)
                
            if not response.success:
                # Push into new queue
                new_queue.append(response)
            else:
                # If it is successful, and was a read, then we need to print
                if next_request.operation == 'R':
                    print(f"{next_request.transaction.name}: {response.x}={response.v}")
                    
        self.request_queue = new_queue
        
        
    def abort(self,T,msg):
        """Abort transaction T at all sites. Aborting involves
        the following steps.
            - Ending the transaction at each site.
            - Removing the transaction from the TransactionManager
            - Removing any requests from this transaction still in
              the request queue

        
        Parameters
        ----------
        T : Transaction
            Transaction being aborted
        msg : str
            Reason for abort (currently either 'failure' or 'deadlock')
            
        Side effects
        ------------
        Remove T from all objects managed by either the
        SiteManagers or the TransactionManager

        See Also
        --------
        src.SiteManager.end
        """
        # End this transaction at all sites
        for site_number,site in self.sites.items():
            site.end(T)

        # Delete this transaction
        del self.transactions[T.name]
        
        # Update the request queue
        new_q = deque()
        while self.backlogged_requests():
            next_t = self.request_queue.popleft()
            if not next_t.transaction == T:
                new_q.append(next_t)
        self.request_queue = new_q
        print(f'Aborting transaction {T.name} due to {msg}')

    def commit(self,T):
        """Commit a read-write transaction T by
            - Writing each site's after-image to the corresponding site
            - Ending the transaction at each site
            - Removing the transaction from the TransactionManager
        
        Parameters
        ----------
        T : Transaction
            Transaction to commit
            
        Side effects
        ------------
        Remove T from all objects managed by either the
        SiteManagers or the TransactionManager

        See Also
        --------
        src.SiteManager.end
        """
        for site in T.after_image:
            # Write to memory
            for x,v in T.after_image[site].items():
                site.write(x,v)
            # Then commit to disk
            site.commit(self.time)
        
        # Remove T from each site. Note we
        # need to do this separately from the above
        # loop, since we need to handle sites where
        # T only read.
        for site in self.sites.values():
            site.end(T)
        
        # Delete this transaction
        del self.transactions[T.name]

        print(f"{T.name} commits")
    
    def try_next_request(self,debug):
        """Try executing the next requested operation.

        Parameters
        ----------
        debug : bool
            If debug, print the next instruction.
        """
        try:
            next_instruction = self.instructions.__next__()
            if debug:
                print(next_instruction)
            self.p_line(next_instruction)
            return False
        except StopIteration:
            return True

    def tick(self,debug=False):
        """Advance time by one step. When we advance time, we:
            1. Detect deadlocks, and abort the youngest transaction in the cycle.
            2. Attempt to execute callbacks in queue
            3. Attempt to execute next instruction

        Parameters
        ----------
        debug : bool
            If True, print each incoming instruction for debugging

        Returns
        -------
        bool
            True if input instructions exhausted, False if not
        """
        
        # Step 0: Increment time
        self.time += 1
        
        # Step 1: Detect deadlocks and abort transactions
        self.unlock_deadlocks()

        # Step 2: Try executing callbacks in queue
        self.sweep_request_queue()
        
        # Step 3: Execute next instruction
        return self.try_next_request(debug)

    def route_read_only_read(self,T,x):
        """ Route a read-only read request from transaction T for variable x.
        To route a read-only read request, we need to find the server satisfying:
            - Server is alive
            - Server has most recently commited, available version of x, subject
              to contraint that the commit was before T's start_time
        If this server exists, then we return the read value. Otherwise
        this transaction is blocked and needs to wait for a server to
        come back online.
        
        Parameters
        ----------
        T : ReadOnlyTransaction
            Read-only transaction requesting a read on x
        x : Variable name
            Variable to read
            
        Returns
        -------
        nts.Request
        
        See Also
        --------
        nts.Request
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
            response = Request(transaction=T,x=x,v=most_recent_value,
                                operation='R',success=True,callback=None,)
        else:
            # Didn't find such a server -- so need to try to find one at next tick
            response = Request(transaction=T,x=x,v=None,
                                operation='R',success=False,callback=lambda time: self.route_read_only_read(T,x))
        return response
        
    def route_read_write_read(self,T,x):
        """Route a read-write read request from transaction T for variable x.
        
        Need to find the server satisfying:
            - Server is alive
            - Variable is available (e.g. not unavailable due to failure)
            - Variable x is not locked
            
        If no servers satisfy these conditions, then we find a server:
            - Server is alive
            - Variable is available (e.g. not unavailable due to failure)
            - Request lock on x and wait
            
        Parameters
        ----------
        T : ReadWriteTransaction
            ReadWriteTransaction requesting read on x
        x : Variable name
            Name of variable to read
            
        Returns
        -------
        nts.Request
        
        See Also
        --------
        nts.Request
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
                    v = T.read(site,x,self.time)
                    return Request(transaction=T,x=x,v=v,
                                    operation='R',success=True,callback=None)
                else:
                    # Then it's locked -- but we still could just wait for the lock
                    live_locked_sites.append((site_number,site))
        
        # If no live site had x unlocked and available, then we need to route to a locked site.
        if len(live_locked_sites)>0:
            site_number,site = random.choice(live_locked_sites)

            # Then we need to put the transaction in the queue to get a RL on x
            waiting_for = site.RL_available(T,x).transactions
            site.add_transaction_to_lock_queue(T,x,waiting_for,'RL')
                    
            # The callback is then T.try_again -- T just needs to waits
            # to get this read lock at this site
            response = Request(transaction=T,x=x,v=None,
                            operation='R',success=False,callback=T.try_again)

            
            # Updated transactions blocked_request
            T.blocked_request = response
            return response
        
        # If there are no live sites with x available, then we need to give up and
        # retry later

        response = Request(transaction=T,x=x,v=None,
                           operation='R',success=False,callback=lambda time: self.route_read_write_read(T,x))

        T.blocked_request = response
        return response
    
        
    def try_reading(self,T,x):
        """Dispatch read request based on whether T is a
        ReadOnlyTransaction or a ReadWriteTransaction.
            
        Parameters
        ----------
        T : Transaction
            Transaction requesting read on x
        x : Variable name
            Name of variable to read
            
        Returns
        -------
        None, but prints to read response message to stdout if successful
            
        See Also
        --------
        route_read_write_read and route_read_only_read
        """
        # Route read request based on transaction type
        if T.read_only:
            response = self.route_read_only_read(T,x)
        else:
            response = self.route_read_write_read(T,x)
                                                 
        if not response.success:
            self.request_queue.append(response)
        else:
            print(f"{T.name}: {x}={response.v}")
            
        
    def try_writing(self,T,x,v):
        """Try writing to all live servers. If a WL is available at all servers,
        then we obtain the WLs and write. Otherwise we need to wait in request queue.
        Then there are two reasons why we wait -- either we've requested locks at all live sites,
        and are waiting for those locks -- or all sites with x were down.
        
        Parameters
        ----------
        T: ReadWriteTransaction
            Transaction requesting write on x
        x : Variable name
            Name of variable to read
        v : Any
            Value to write to x
            
        Returns
        ----------
        nts.Request
            
        Side effects
        ------------
        - If all live sites provide WL, then T obtains the locks and writes to
          its after image of each available site.
        - If no sites with x are live, then we we'll try later, adding request
          to the request queue
        - If there are available copies, but they're locked, then we wait on
          those locks and a try again request to the request queue.
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
                    T.write(site,x,v,self.time)
            return Request(transaction=T,x=x,v=v,
                           operation='W',success=True,callback=None)
        elif not at_least_one_available_site:
            # If no available sites then we just need to give up and try again later
            response = Request(transaction=T,x=x,v=v,
                               operation='W',success=False,callback=lambda time: self.try_writing(T,x,v))

            T.blocked_request = response
            self.request_queue.append(response)
        
        else:
            # Otherwise there are available sites -- but we can't get all of the locks
            # so we simply add the lock request to the queue at each live site
                                
            for site_number,site in self.sites.items():
                # Ignore sites that don't contain x or are down
                if (site.alive) and (x in site.memory):
                    waiting_for = site.WL_available(T,x).transactions
                    site.add_transaction_to_lock_queue(T,x,waiting_for,'WL')
        
            # The callback is then T.try_again -- T just needs to waits
            # to be given all the requested locks
            response = Request(transaction=T,x=x,v=v,
                                operation='W',success=False,callback=lambda time: T.try_again(time))

            T.blocked_request = response
            self.request_queue.append(response)
        
        