import fileinput
from collections import namedtuple,deque
import re
import networkx as nx
import random
import argparse
import sys
import fileinput

from .site_manager import SiteManager
from .parser import Parser
from .transaction import ReadWriteTransaction,ReadOnlyTransaction
from .request_response import RequestResponse

class TransactionManager(Parser):
    """
    The transaction manager is responsible for translating read and write
    requests on variables to read and write re-quests on copies using
    the available copy algorithm. It both parses and routes incoming
    requests, and manages a queue of requests.
    
    Attributes
    ----------
    time : int
        Current time (number of ticks)
    sites : dict of int : SiteManager
        Maps site numbers (1-10) to SiteManagers
    transactions : dict of str : Transaction
        Maps transaction names (ex: T1) to Transactions
    instructions : TextIOWrapper
        Wrapper on input file, or stdin, from which instructions are read
    request_queue : deque
        A queue storing RequestResponses, with callbacks to execute for operations
        that were blocked by lock conflicts or failures
    failed_request_queue : deque
        A second queue storing RequestResponses. Stores responses for
        requests which are unsuccessfully called during a tick. 
    """
    N_SITES = 10
    """Total number of sites.
    """
    
    def __init__(self,instructions='-'):
        """Initializes the TransactionManager for the input
        transactions specified by `instructions`.

        Parameters
        ----------
        instructions : Input instructions
            List of files to parse as instructions. By default '-'
            (i.e. sys.stdin).
        """
        super().__init__()
        self.time = 0
        self.sites = {i:SiteManager(i,self.time) for i in range(1,self.N_SITES+1)}
        self.transactions = {}
        self.instructions = instructions
        self.request_queue = deque()
        self.failed_request_queue = deque()
    
    def live_sites_with_x(self,x):
        """Get list of live sites with x.

        Parameters
        ----------
        x : Variable name

        Returns
        -------
        List of SiteManagers
            List of live sites with x
        """
        live_w_x = []
        for site_number, site in self.sites.items():
            if (site.alive) and (x in site.memory):
                live_w_x.append(site)
        return live_w_x

    def main(self,debug=False):
        """Main function that reads entire set of input commands.
        """
        done = False
        while not done:
            done = self.tick(debug)

    def tick(self,debug=False):
        """Advance time by one step. When we advance time, we:
            1. Detect deadlocks, and abort the youngest transaction in the cycle.
            2. Reads next instruction and adds corresponding RequestResponse to end of queue.
            3. Attempt to execute RequestReponses in queue

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

        # Step 2: Get next request and add to request queue
        new_request = self.get_next_new_request(debug)
        if new_request is not None:
            self.request_queue.append(new_request)
            end_of_instructions = False
        else:
            end_of_instructions = True

        # Step 3: Try executing callbacks in queue
        self.sweep_request_queue()

        return end_of_instructions

    def sweep_request_queue(self):
        """Sweep through request queue in order, calling callbacks. 
        Note the request queue includes blocked operations,
        where the requested operation is blocked due to either lock
        conflicts or due to failure, as well as the next new instruction.

        Notes
        ------------
        - See side effects for functions called during commit/abort/read/write/etc.
        """    
        while self.has_backlogged_requests():

            next_request = self.request_queue.popleft()

            # Execute callback
            response = next_request.callback(next_request,self.time)
            if not response.success:
                # Push response onto new queue
                self.failed_request_queue.append(response)
            else:
                # If it is successful, and was a read, then we need to print
                if next_request.operation == 'R':
                    print(f"{next_request.transaction.name}: {response.x}={response.v}")
                    
        self.request_queue = self.failed_request_queue
        self.failed_request_queue = deque()
       
    def has_backlogged_requests(self):
        """Check if the TransactionManager still needs to try working
        through backlogged requests.

        Returns
        -------
        bool
            True if the request queue is not empty.
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
        src.request_response.RequestResponse
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

        Notes
        ------------
        - If a deadlock is detected, aborts the youngest transaction
          in the detected cycle.
        - When a cycle is aborted, each site attempts to reassign locks
          to queued transactions.
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
        composed_waits_for : networkx.DiGraph
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
        networkx.DiGraph
            Composed waits for graph from each site
        """

        composed = nx.algorithms.operators.all.compose_all(
            [s.waits_for for s in self.sites.values()]
        )
        return composed

    def manage_write_request(self,request,time):
        """Manage a write request. Updates lock requests for the requesting
        transaction (i.e. to ensure that locks have been requested at all
        available copies). If there are available copies, have the site
        write if it is holding all required locks; if it isn't holding
        all required locks, then it waits. If there are no available copies,
        transaction needs to wait until there are available copies.

        Parameters
        ----------
        request : RequestResponse
            A "W" RequestResponse.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.

        Returns
        -------
        RequestResponse
            A new RequestResponse object.

        Notes
        ------------
        - Check that the operation is waiting for the write lock at all live sites
          with the variable. If not, request locks as needed so the write operation
          will write to all available copies.
        """
        # Step 1: Update the lock requests
        self.refresh_transactions_write_lock_requests(request)

        # Step 2(a): If no available sites (i.e. not waiting on any locks)
        # then wait till next tick
        if not request.transaction.is_waiting_on_locks():
            return request

        # Step 2(b): If waiting on available sites (i.e. waiting on locks)
        # then write if holding all locks
        else:
            response = request.transaction.write_if_holding_all_required_locks(request,self.time)
         
        return response

    def refresh_transactions_write_lock_requests(self,request):
        """Check if the write transaction has lock requests in at all
        available sites. If not, request missing locks.

        Parameters
        ----------
        request : RequestResponse, for a "W" operation

        Notes
        ------------
        - Either adds the transaction to the lock queue, or gives the transaction
          the write lock, at each site which is (i) alive, and (ii) the
          transaction is not holding or waiting for a lock.
        """
        for site in self.live_sites_with_x(request.x):
            # Check that the transaction knows it needs the lock on site.x
            # and request lock if not
            site.request_write_lock(request.transaction,request.x)
      
    def manage_read_write_read_request(self,request,time):
        """Manage a read request from a read-write transaction. Check
        if the transaction is waiting on a lock. If so, then read
        if its holding the lock. If it is not waiting on a lock,
        then it needs to be re-routed.

        Parameters
        ----------
        request : RequestResponse
            A "R" RequestResponse.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.

        Returns
        -------
        src.request_response.RequestResponse
            Response to request.

        See Also
        --------
        :py:meth:`~src.transaction.ReadWriteTransaction.read_if_holding_all_required_locks`
        """
        # Read-write transaction
        if not request.transaction.is_waiting_on_locks():
            # Then need to re-route to a live site
            response = self.route_read_write_read(request,time)
        else:
            # Waiting on a lock at a live site.
            response = request.transaction.read_if_holding_all_required_locks(request,time)

        return response

    def manage_read_only_read_request(self,request,time):
        """ Route a read-only read request from transaction T for variable x.
        To route a read-only read request, we need to find a server satisfying:
            - Server is alive
            - Server has most recently commited, available version of x, subject
              to contraint that the commit was before T's start_time. Note
              we can guarantee this if three events can be ordered as
              `Site goes live` <= `[`Snapshot commit with x.available`]` <= `T starts`.
              Note the equality isn't important here, since a commit only occurs
              when a transaction ends, and end(T) and beginRO(T') will always
              occur on separate ticks.
        If this server exists, then we return the read value. Otherwise
        there are two cases:
            - There are no live servers -- so we just wait
            - There are live servers, but none satify the conditions outlined
              above. Then we need to wait until we can try reading from all
              servers to guarantee we correctly identify the most recent commit to x.
        
        Parameters
        ----------
        request : RequestResponse
            A "R" RequestResponse from a read-only transaction.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.
            
        Returns
        -------
        src.request_response.RequestResponse
            Response to request.

        See Also
        --------
        :py:meth:`~src.site_manager.SiteManager.try_reading_from_disk`
        """
        for site in self.live_sites_with_x(request.x):
            response = site.try_reading_from_disk(request)
            if response.success:
                # Then we found a site with the most recent value of x,
                # which can respond to read request
                return response
            
            elif response.success is None:
                # Then the site returned its most recent available, commited
                # value of x -- but the site can't guarantee this is the
                # most recent commit to x (due to intervening site failure).
                # The read-only transaction thus stores the (potentially
                # incorrect) read value of x at the site, along with the associated
                # commit time, and the transaction will only complete the transaction
                # when it has checked all sites.
                most_recent_val_and_time = response.v
                response.transaction.read_values_and_times[site] = most_recent_val_and_time

                # Check if this transaction has now obtained the most recent
                # available commit from all such sites. If so, return
                # the most recent value
                if response.transaction.has_read_x_at_all_sites():
                    v = response.transaction.most_recent_commit_to_x()
                    new_response = RequestResponse(transaction=response.transaction,
                                                   x=response.x,v=v,
                                                   operation='R',success=True,
                                                   callback=None)
                    return new_response
        
        # Didn't find such a server -- so need to try to find one at next tick
        return request
        
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
        request : RequestResponse
            A "R" RequestResponse.
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.
            
        Returns
        -------
        src.request_response.RequestResponse
            Response to request.
        """

        T = request.transaction
        x = request.x

        # We'll store a list of locked, live sites containing x in case
        # we need to route read to one of these sites
        live_locked_sites = []
        
        for site in self.live_sites_with_x(x):
            if site.memory[x].available:
                if site.RL_available(T,x).response:
                    # Then lock is available so T can get the lock ...
                    RL = site.give_transaction_RL(T,x)
                    # ... and read the value
                    v = T.read_site_x(site,x,self.time)
                    return RequestResponse(transaction=T,x=x,v=v,
                                           operation='R',success=True,
                                           callback=None)
                else:
                    # Then it's locked -- so we could wait for the lock at this site
                    live_locked_sites.append(site)
        
        # If no live site had x unlocked and available, then we need to route to a locked site.
        if len(live_locked_sites)>0:
            # Choose a live locked site
            site = random.choice(live_locked_sites)

            # Put the transaction in the queue to get a RL on x
            waiting_for = site.RL_available(T,x).transactions
            site.add_transaction_to_lock_queue(T,x,waiting_for,'RL')
                    
        return request
            
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
            
        Notes
        ------------
        - Remove T from all objects managed by either the SiteManagers
          or the TransactionManager

        See Also
        --------
        :py:meth:`~src.site_manager.SiteManager.end`
        """
        # End this transaction at all sites
        for site_number,site in self.sites.items():
            site.end(T)

        # Delete this transaction
        del self.transactions[T.name]

        # Update the request queue.
        new_q = deque()
        while self.has_backlogged_requests():
            next_t = self.request_queue.popleft()
            if not next_t.transaction == T:
                new_q.append(next_t)
        self.request_queue = new_q

        # Update the failed request queue.
        new_q = deque()
        while len(self.failed_request_queue) > 0:
            next_t = self.failed_request_queue.popleft()
            if not next_t.transaction == T:
                new_q.append(next_t)
        self.failed_request_queue = new_q

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
            
        Notes
        ------------
        - Remove T from all objects managed by either the
          SiteManagers or the TransactionManager

        See Also
        --------
        :py:meth:`~src.site_manager.SiteManager.end`
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
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='RepCRec simulation.')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin)

    args = parser.parse_args()
    TM = TransactionManager(args.infile)
    TM.main()