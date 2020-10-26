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
    
    def main(self,debug=False):
        """Main function that reads entire input command.
        """
        done = False
        while not done:
            done = self.tick(debug)

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
        end_of_instructions = False
        
        # Step 0: Increment time
        self.time += 1
        
        # Step 1: Detect deadlocks and abort transactions
        self.unlock_deadlocks()

        # Step 2: Get next request and add to request queue
        new_request = self.get_next_new_request(debug)
        if new_request is not None:
            self.request_queue.append(new_request)
        else:
            end_of_instructions = True

        # Step 3: Try executing callbacks in queue
        self.sweep_request_queue()

        return end_of_instructions

    def sweep_request_queue(self):
        """Sweep through request queue in order, calling callbacks. 
        Note the request queue is a queue of blocked operations,
        where the requested operation is blocked due to either lock
        conflicts or due to failure.
        """
        new_queue = deque()
    
        while self.has_backlogged_requests():

            next_request = self.request_queue.popleft()

            # Execute callback
            response = next_request.callback(next_request,self.time)
            if not response.success:
                # Push response onto new queue
                new_queue.append(response)
            else:
                # If it is successful, and was a read, then we need to print
                if next_request.operation == 'R':
                    print(f"{next_request.transaction.name}: {response.x}={response.v}")
                    
        self.request_queue = new_queue
       
    def has_backlogged_requests(self):
        """Has backlogged requests
        """
        return len(self.request_queue)>0

    def get_next_new_request(self,debug):
        """Get next request

        Parameters
        ----------
        debug : bool
            If debug, print the next instruction.

        Returns
        -------
        Request
            Next request. If None, then simulation is over.
        """
        try:
            next_instruction = self.instructions.__next__()
            if debug:
                print(next_instruction)
            request = self.parse_line(next_instruction)
            return request
        except StopIteration:
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
                self.abort_transaction(youngest_transaction_in_cycle,'deadlock')

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
        cycle = self.get_waits_for_cycle(composed_waits_for)

        if cycle is not None:
            ordered_by_age = sorted(cycle, key=lambda x: x.start_time)
            youngest_transaction = ordered_by_age[-1]
            return youngest_transaction
        else:
            return None
    
    def get_waits_for_cycle(self,composed_waits_for):
        """
        Return a cycle in the composed waits_for graph, if one exists.
        
        Parameters
        ----------
        composed_waits_for : digraph
            Composition of all of the waits-for graphs
            
        Returns
        -------
        set
            Set of transactions in the detected cycle in the waits for graph.
            None if no cycles in the composed waits for graph.
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

    def manage_write_request(self,request,time):
        """Manage a write request. Updates the lock requests for the requesting
        transaction (i.e. to ensure that locks have been requested at all
        available copies). If there are available copies, have the site
        write if it is holding all required locks; if it isn't holding
        all required locks, then it waits. If there are no available copies,
        transaction needs to wait until there are available copies.

        Parameters
        ----------
        request : Request
            A "W" Request.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.

        Returns
        -------
        Request
            A new Request object.

        Side effects
        ------------
        - If the operation is a write, check that the operation is waiting
          for the write lock at all live sites with the variable. If not,
          request locks as needed so the write operation will write to all
          available copies.
        """
        # Step 1: Update the lock requests
        self.refresh_transactions_write_lock_requests(request)

        # Step 2(a): If no available sites (i.e. not waiting on any locks)
        # then wait till next tick
        if not request.transaction.is_waiting_on_locks():
            response = Request(transaction=T,x=x,v=v,
                               operation='W',success=False,
                               callback=self.manage_write_request)

        # Step 2(b): If waiting on available sites (i.e. waiting on locks)
        # then write if holding all locks
        else:
            response = request.transaction.write_if_holding_all_required_locks(request,self.time)
         
        return response

    def refresh_transactions_write_lock_requests(self,next_request):
        """Check if the write transaction has lock requests in at all
        available sites. If not, request missing locks.

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
      
    def manage_read_write_read_request(self,request,time):
        """Manage a read request. If the requesting transaction is read only,
        then we simply route the request. If it's read-write, check if the
        transaction is waiting on a lock.

        Parameters
        ----------
        request : Request
            A "R" Request.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.

        Returns
        -------
        Request
            A new Request object.
        """
        # Handle read only transactions
        if request.transaction.read_only:
            response = next_request.callback(request,self.time)
            return response
        else:
            # Read-write transaction
            if not request.transaction.is_waiting_on_locks():
                # Then need to re-route to a live site
                response = self.route_read_write_read(request,time)
            else:
                # Waiting on a lock at a live site.
                response = request.transaction.read_if_holding_all_required_locks(request,self.time)

        return response

    def manage_read_only_read_request(self,request,time):
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
        request : Request
            A "R" Request from a read-only transaction.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.
            
        Returns
        -------
        nts.Request
        
        See Also
        --------
        nts.Request
        """
        T = request.transaction
        x = request.x

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
                                operation='R',success=True,callback=None)
        else:
            # Didn't find such a server -- so need to try to find one at next tick
            response = Request(transaction=T,x=x,v=None,
                                operation='R',success=False,
                                callback=self.route_read_only_read)
        return response
        
    def route_read_write_read(self,request,time):
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
        request : Request
            A "R" Request.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.
            
        Returns
        -------
        nts.Request
        
        See Also
        --------
        nts.Request
        """

        T = request.transaction
        x = request.x

        # We'll store a list of locked, live sites containing x in case
        # we need to route read to one of these sites
        live_locked_sites = []
        
        for site_number,site in self.sites.items():
            # Ignore sites that don't contain x, are down, or have
            # recovered without a write to x
            if (site.alive) and (x in site.memory) and (site.memory[x].available):
                if site.RL_available(T,x).response:
                    # Then lock is available so T can get the lock ...
                    RL = site.give_transaction_RL(T,x)
                    # ... and read the value
                    v = T.read_site_x(site,x,self.time)
                    return Request(transaction=T,x=x,v=v,
                                   operation='R',success=True,
                                   callback=None)
                else:
                    # Then it's locked -- so we could wait for the lock at this site
                    live_locked_sites.append((site_number,site))
        
        # If no live site had x unlocked and available, then we need to route to a locked site.
        if len(live_locked_sites)>0:
            site_number,site = random.choice(live_locked_sites)

            # Then we need to put the transaction in the queue to get a RL on x
            waiting_for = site.RL_available(T,x).transactions
            site.add_transaction_to_lock_queue(T,x,waiting_for,'RL')
                    
        response = Request(transaction=T,x=x,v=None,
                            operation='R',success=False,
                            callback=self.manage_read_write_read_request)
        
        return response 
            
    def abort_transaction(self,T,msg):
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
        while self.has_backlogged_requests():
            next_t = self.request_queue.popleft()
            if not next_t.transaction == T:
                new_q.append(next_t)
        self.request_queue = new_q
        print(f'Aborting transaction {T.name} due to {msg}')

    def commit_transaction(self,T):
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
    