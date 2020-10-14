from collections import deque,namedtuple
import networkx as nx

LockRequest = namedtuple('LockRequest', ['transaction', 'lock_type'])
LockTableResponse = namedtuple('LockTableResponse', ['response', 'transactions'])

class LockTable(object):
    """LockTable for a single site.
    """
    def __init__(self,variables,site_number):
        
        self.site_number = site_number
        self.lock_table = {
            x: {
                'RL': set(),
                'WL': None,
                'waiting': deque()
            } for x in variables
        }
        
        self.waits_for = nx.DiGraph()
        
    def RL_available(self,T,x):
        """
        Check if transaction T can get a read lock on x. A transaction
        can get a read lock if:
            - It already holds a read lock or a write lock on x
            - It does not already hold a sufficient lock, and no
              other transactions are either (i) currently holding a 
              write lock, or (ii) has a write-lock request waiting in the
              queue.
        
        Args: 
            T: transaction requesting RL
            x: variable for which RL is requested
            
        Returns:
            bool: True if RL available, false if not
            set: Set of transactions for which T would need to wait for RL.
                 Empty if lock is available.
            
        Side effects:
            None                
        """
        holding_write_lock = self.lock_table[x]['WL']
        waiting_for_write_lock = set(other.transaction for other in self.lock_table[x]['waiting']
                                     if other.lock_type == 'WL')
        
        # Case 1 -- another transaction is currently holding a write lock
        if (holding_write_lock is not None) and (holding_write_lock != T):
            return LockTableResponse(
                response=False,
                transactions=set([holding_write_lock]).union(waiting_for_write_lock)
            )
        
        # Case 2 -- no other transaction is currently holding a write lock,
        # but some other transaction is waiting for a write lock
        elif (holding_write_lock is None) and (len(waiting_for_write_lock.difference([T])) > 0):
            return LockTableResponse(
                response=False,
                transactions=set([holding_write_lock]).union(waiting_for_write_lock)
            )
        
        # Case 3: all other cases, T can have lock
        else:
            return LockTableResponse(response=True,
                                     transactions=set())
        
    def WL_available(self,T,x):
        """
        Check if transaction T can get write lock on x. A transaction can get a write
        lock on x if
            - it already holds a write-lock on x
            - it holds a read-lock on x, and there are no other transactions waiting
              in the queue for a lock on x. Note it would be possible for T to hold
              a RL on x, and thus create a queue (if there is a T' requesting a WL,
              followed by any sequence of transactions)
            - No transactions are holding either a read or write lock on x
        In any other case, (i) some transaction has lock on x, and/or (ii) other
        transactions are waiting for a lock -- so this lock request waits for
        all other transactions.
        
        Args: 
            T: transaction requesting WL
            x: variable for which WL is requested
            
        Returns:
            bool: True if WL available, false if not
            set: Set of transactions for which T would need to wait for the WL.
                 Empty if lock is available.
            
        Side effects:
            None
        """
        
        # Case 1 -- T already has WL
        if (self.lock_table[x]['WL'] == T):
            return LockTableResponse(response=True,
                                     transactions=set())
            
        # Case 2 -- T already has RL and no T' are waiting for locks
        if ((T in self.lock_table[x]['RL']) and 
            (len(self.lock_table[x]['waiting'].difference([T])) == 0)):
            return LockTableResponse(response=True,
                                     transactions=set())
            
        # Case 3 -- no locks held and no waiters
        if ((self.lock_table[x]['WL'] is None) and 
            (len(self.lock_table[x]['RL']) == 0) and 
            (len(self.lock_table[x]['waiting']) == 0)):
            return LockTableResponse(response=True,
                                     transactions=set())
            
        # All other cases, T needs to wait for any non-T transaction that is
        # currently holding or waiting for a lock on x
        
        waiting_for = set()
        
        # Waits for transaction holding write lock
        holding_write_lock = self.lock_table[x]['WL']
        if (holding_write_lock is not None):
            waiting_for = set([holding_write_lock])
        
        # Waits for any other transaction currently holding RL on x
        other_transactions_w_RLs = self.lock_table[x]['RL'].difference([T])
        if other_transactions_w_RLs is not None:
            waiting_for = waiting_for.union(other_transactions_w_RLs)
            
        # Waits for any other transactions waiting for a lock
        other_transactions_waiting = set(self.lock_table[x]['waiting']).difference([T])
        if other_transactions_waiting is not None:
            waiting_for = waiting_for.union(other_transactions_waiting)\
            
        return LockTableResponse(
            response=False,
            transactions=waiting_for
        )
        
        
    def give_transaction_WL(self,T,x):
        """
        Gives transaction T a WL on x.
        
        Args: 
            T: transaction requesting lock
            x: variable for which lock is requested
            
        Returns:
            None
            
        Side effects:
            - Set WL entry to T
            - Add this site to T's WLs
            
        """
        self.lock_table[x]['WL'] = T
        T.write_locks[self].add(x)
    
    def give_transaction_RL(self,T,x):
        """
        Gives transaction T a RL on x.
        
        Args: 
            T: transaction requesting lock
            x: variable for which lock is requested
            
        Returns:
            None
            
        Side effects:
            - Add T to x's RL set
            - Add site.x to T's currently held read_locks
        """
        self.lock_table[x]['RL'].add(T)
        T.read_locks[self].add(x)
        
    def release_WL(self,T,x):
        """
        Release transaction T's WL on x. Note this
        does not trigger the transfer of the lock to the
        next transaction in the queue. That waits for the end
        of the tick.
        
        Args:
            T: Transaction holding WL on x
            x: variable for which lock is hold
            
        Returns:
            None
            
        Side effects:
            - Releases lock
        """
        self.lock_table[x]['WL'] = None
        T.write_locks[self].remove(x)
    
    def release_RL(self,T,x):
        """
        Release transaction T's RL on x. Note this
        does not trigger the transfer of the lock to the
        next transaction in the queue. That waits for the end
        of the tick.
        
        Args:
            T: Transaction holding RL on x
            x: variable for which lock is hold
            
        Returns:
            None
            
        Side effects:
            - Releases lock
        """
        self.lock_table[x]['RL'].remove(T)
        T.read_locks[self].remove(x)
        
    def add_transaction_to_lock_queue(self,T,x,waiting_for,lock_type):
        """
        Adds transaction T to the queue for a lock on variable x.
        
        Args: 
            T: transaction requesting lock
            x: variable for which lock is requested
            waiting_for: set of transactions blocking T's request for the x lock
            lock_type: type of lock being requested (WL or RL)
            
        Returns:
            None
            
        Side effects:
            - Adds the T's lock request on x to x's queue
            - Adds edges to the waits-for graph from T to each transaction in waiting_for
        """
        self.lock_table[x]['waiting'].append(LockRequest(transaction=T,lock_type=lock_type))
        for other in waiting_for:
            self.waits_for.add_edge(T,other)
        
    def waits_for_has_cycles(self):
        """
        Check for cycles in the waits_for graph.
        
        Args:
            None
            
        Returns:
            bool: True if waits_for graph has cycles
            set: Set of transactions in the detected cycle in the waits for graph.
                 Empty set if no cycles
                 
        Side effects:
            None
        """
        try:
            cycle = nx.algorithms.cycles.find_cycle(self.waits_for)
            nodes_in_cycle = set()
            for e in cycle:
                for task in e:
                    nodes_in_cycle.add(task)
            return LockTableResponse(response=True,transactions=nodes_in_cycle)
        except nx.NetworkXNoCycle:
            return LockTableResponse(response=False,transactions=set())
    
    def reassign_locks(self,x):
        """
        Reassign locks (as needed) for variable x
        
        Args:
            x: variable to reassign locks
            
        Returns:
            None
            
        Side effects:
            If locks reassigned, updates the locks held by transactions
            given new locks
        """
        gave_new_lock = True
        waiters = self.lock_table[x]['waiting']
        while gave_new_lock and (len(waiters)>0):
            next_in_line = self.lock_table[x]['waiting'].popleft()
            next_lock_type = next_in_line.lock_type
            requesting_T = next_in_line.transaction
            
            if (next_lock_type == 'RL'):
                # Case 1: No transaction holds a WL
                if (self.lock_table[x]['WL'] is None):
                    self.give_transaction_RL(requesting_T,x)
                # Case 2: requesting_T already has WL
                elif (self.lock_table[x]['WL'] == requesting_T):
                    self.give_transaction_RL(requesting_T,x)
                # Case 3: some other transaction has WL
                else:
                    gave_new_lock = False
                    # Put this transaction back at front of queue
                    self.lock_table[x]['waiting'].appendleft(next_in_line)
                    
            # Requesting WL
            else:
                # Case 1: No other transaction holds a WL and no other transactions have
                # read locks
                if ((self.lock_table[x]['WL'] is None) and
                    (len(self.lock_table[x]['RL'].difference([requesting_T]))==0)):
                    self.give_transaction_WL(requesting_T,x)
                # Otherwise we can't give out lock
                else:
                    gave_new_lock = False
                    # Put this transaction back at front of queue
                    self.lock_table[x]['waiting'].appendleft(next_in_line)
        
        
    def remove_T_from_lock_table_and_waitsfor(self,T):
        """
        Completely removes T from the lock table and waits
        for graph, and updates the lock table (reassigning locks).
        
        Args: 
            T: transaction requesting lock
            
        Returns:
            None
            
        Side effects:
            - Remove T from the lock table.
            - Remove T from the waits_for graph
        """
        for x,row in self.lock_table.items():
            # Remove T from the RL set if it is present
            row['RL'].discard(T)
            # Reset the WL entry to None if T was holding WL
            if row['WL'] == T:
                row['WL'] = None
            # Remove T from the queue for locks on x
            new_wait_list = deque()
            for waiter in row['waiting']:
                if waiter[0] != T:
                    new_wait_list.append(waiter)
            row['waiting'] = new_wait_list
            
            # Then reassign locks
            self.reassign_locks(x)
        
        # Finally remove node from waits_for graph
        if T in self.waits_for.nodes():
            self.waits_for.remove_node(T)