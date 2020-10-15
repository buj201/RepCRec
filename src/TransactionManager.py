import fileinput
from collections import namedtuple,deque
import re
import networkx as nx
import random

from src.SiteManager import SiteManager
from src.Parser import Parser
from src.Transaction import ReadWriteTransaction,ReadOnlyTransaction
from src.nts import WriteOp,ReadOp,ReadResponse,WriteResponse

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
        self.transaction_queue = deque()
    
    def main(self):
        """Main function that reads entire input command.
        """
        done = False
        while not done:
            done = self.tick()

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
        """ Detect cycles in waits-for graph. If cycle(s)
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
        
    def sweep_transaction_queue(self):
        """ Sweep through transaction queue in order, calling callbacks. 
        Note the transaction queue is a queue of blocked operations,
        where the requested operation might be blocked due to lock
        conflicts or due to failure.
        
        Side effects
        ------------
        Removes transaction request from queue if the callback is
        successful.
        """
        new_queue = deque()
        while len(self.transaction_queue) > 0:
            next_transaction = self.transaction_queue.popleft()
            response = next_transaction.callback(self.time)
            if not response.success:
                new_queue.append(next_transaction)
        self.transaction_queue = new_queue
        
        
    def abort(self,T,msg):
        """ Abort transaction T at all sites. Aborting involves
        the following steps.
            - Ending the transaction at each site.
            - Removing the transaction from the TransactionManager
            - Removing any requests from this transaction still in
              the transaction queue

        
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
        
        # Update the transaction queue
        new_q = deque()
        while (len(self.transaction_queue)>0):
            next_t = self.transaction_queue.popleft()
            if not next_t.transaction == T:
                new_q.append(next_t)
        self.transaction_queue = new_q
        print(f'Aborting transaction {T.name} due to {msg}')

    def commit(self,T):
        """ Commit a read-write transaction T by
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
            site.end(T)
        
        # Delete this transaction
        del self.transactions[T.name]

        print(f"{T.name} commits")
            
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
        deadlocked = True
        while deadlocked:
            youngest_transaction_in_cycle = self.identify_youngest_transaction_in_cycle()
            if youngest_transaction_in_cycle is None:
                deadlocked = False
            else:
                self.abort(youngest_transaction_in_cycle,'deadlock')
        
        # Step 2: Try executing callbacks in queue
        self.sweep_transaction_queue()
        
        # Step 3: Execute next instruction
        try:
            next_instruction = self.instructions.__next__()
            if debug:
                print(next_instruction)
            self.p_line(next_instruction)
            return False
        except StopIteration:
            return True

    def route_read_only_read(self,T,x):
        """  Route a read-only read request from transaction T for variable x.
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
        ----------
        ReadResponse
            ReadResponse indicating success, value (if success), and callback (if failure)
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
                                    callback=lambda time: self.route_read_only_read(T,x),
                                    transaction=T)
        return response
        
    def route_read_write_read(self,T,x):
        """ Route a read-write read request from transaction T for variable x.
        
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
        ----------
        ReadResponse
            ReadResponse indicating success, value (if success), and callback (if failure)
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
                                        value=T.read(site,x,self.time),
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
            return ReadResponse(success=False,
                                value=None,
                                callback=lambda time: T.try_again(time),
                                transaction=T)
        
        # If there are no live sites with x available, then we need to give up and
        # retry later
        return ReadResponse(success=False,value=None,
                            callback=lambda time: self.route_read_write_read(T,x),
                            transaction=T)
    
        
    def try_reading(self,T,x):
        """ Dispatch read request based on whether T is a
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
            self.transaction_queue.append(response)
        else:
            print(f'{T.name}: {x}={response.value}')
            
        
    def try_writing(self,T,x,v):
        """ Try writing to all live servers. If a WL is available at all servers,
        then we obtain the WLs and write. Otherwise we need to wait in transaction queue.
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
        WriteResponse
            WriteResponse indicating success, with callback if failure.
            
        Side effects
        ------------
        - If all live sites provide WL, then T obtains the locks and writes to
          its after image of each available site.
        - If no sites with x are live, then we we'll try later, adding request
          to the transaction queue
        - If there are available copies, but they're locked, then we wait on
          those locks and a try again request to the transaction queue.
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
            return WriteResponse(success=True,callback=None,transaction=T)
        elif not at_least_one_available_site:
            # If no available sites then we just need to give up and try again later
            response = WriteResponse(success=False,
                                     callback=lambda time: try_writing(T,x,v,time),
                                     transaction=T)
            self.transaction_queue.append(response)
        
        else:
            # Otherwise there are available sites -- but we can't get all of the locks
            # so we simply add the lock request to the queue at each live site
            
            # Updated transactions next_op
            T.next_op = WriteOp(x,v)
                    
            for site_number,site in self.sites.items():
                # Ignore sites that don't contain x or are down
                if (site.alive) and (x in site.memory):
                    waiting_for = site.WL_available(T,x).transactions
                    site.add_transaction_to_lock_queue(T,x,waiting_for,'WL')
                    T.locks_needed[site].add(x)
        
            # The callback is then T.try_again -- T just needs to waits
            # to get this read lock at this site
            response = WriteResponse(success=False,
                                     callback=lambda time: T.try_again(time),
                                     transaction=T)
            self.transaction_queue.append(response)
        
        