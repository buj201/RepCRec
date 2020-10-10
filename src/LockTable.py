from collections import deque,namedtuple
import networkx as nx

LockRequest = namedtuple('LockRequest', ['transaction', 'lock_type'])
LockTableResponse = namedtuple('LockTableResponse', ['response', 'transactions'])
WL = namedtuple('WL', ['site', 'variable'])
RL = namedtuple('RL', ['site', 'variable'])

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
        Check if transaction T can get read lock on x. A transaction
        can get a read lock if:
            - It already holds a read lock or a write lock
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
        if (holding_write_lock is not None) and (holding_write_lock != T.name):
            return LockTableResponse(
                response=False,
                transactions=set([holding_write_lock]).union(waiting_for_write_lock)
            )
        
        # Case 2 -- no other transaction is currently holding a write lock,
        # but some other transaction is waiting for a write lock
        elif (holding_write_lock is None) and (len(waiting_for_write_lock.difference(T.name)) > 0):
            return LockTableResponse(
                response=False,
                transactions=set([holding_write_lock]).union(waiting_for_write_lock)
            )
        
        # Case 3: all other cases, T can have lock
        else:
            # Then we can give transaction T the read lock on x
            return LockTableResponse(response=True,
                                     transactions=set())
        
    def WL_available(self,T,x):
        """
        Check if transaction T can get write lock on x. A transaction can get a write
        lock on x if
            - it already holds a write-lock on x
            - it holds a read-lock on x, and there are no other transactions 
                (i) currently holding read locks on x
                (ii) waiting for a write-lock on x
        
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
        waiting_for = set()
        
        # First check if any other transactions are holding a WL on x
        holding_write_lock = self.lock_table[x]['WL']
        if (holding_write_lock is not None) and (holding_write_lock != T.name):
            waiting_for = set([holding_write_lock])
        
        # Next check if any other transactions are holding a RL on v
        other_transactions_w_RLs = self.lock_table[x]['RL'].difference([T.name])
        if other_transactions_w_RLs is not None:
            waiting_for = waiting_for.union(other_transactions_w_RLs)
        
        if len(waiting_for) > 0:
            # Some other transactions holding locks blocking WL, so
            # clearly can't get lock. But, if T wants the lock, it
            # will also have to wait for any other transactions that are waiting
            # in the queue
            waiting_for_lock = set(other.transaction for other in self.lock_table[x]['waiting'])
            return LockTableResponse(
                response=False,
                transactions=waiting_for.union(waiting_for_lock)
            )
        
        else:
            # Then there are no other transactions currently holding read or write locks.
            # But it could be the case that T holds a read lock, T' is waiting on
            # a write lock, and thus T cannot promote its read lock to a write lock
            waiting_for_lock = set(other.transaction for other in self.lock_table[x]['waiting']
                                   if other.lock_type == 'WL')
            
            if len(waiting_for_lock)>0:
                return LockTableResponse(
                    response=False,
                    transactions=waiting_for_lock
                )
            else:          
                return LockTableResponse(response=True,
                                         transactions=set())
        
    def give_transaction_WL(self,T,x):
        """
        Gives transaction T a WL on x.
        
        Args: 
            T: transaction requesting lock
            x: variable for which lock is requested
            
        Returns:
            None
            
        Side effects:
            - Set v's WL entry to T
        """
        self.lock_table[x]['WL'] = T.name
        return WL(site=self.site_number,variable=x)
    
    def give_transaction_RL(self,T,x):
        """
        Gives transaction T a RL on x.
        
        Args: 
            T: transaction requesting lock
            x: variable for which lock is requested
            
        Returns:
            None
            
        Side effects:
            - Add T to x's RL list
        """
        self.lock_table[x]['RL'].add(T.name)
        return RL(site=self.site_number,variable=x)
            
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
        self.lock_table[x]['RL'].remove(T.name)
        
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
        self.lock_table[x]['waiting'].append(LockRequest(transaction=T.name,lock_type=lock_type))
        for other in waiting_for:
            self.waits_for.add_edge(T.name,other)
        
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
               
    def scrub_transaction_from_table(self,T):
        """
        Completely scrubs T from the lock table.
        
        Args: 
            T: transaction requesting lock
            
        Returns:
            None
            
        Side effects:
            - Scrub T from the lock table.
            - Remove T from the waits_for graph
        """
        for x,row in self.lock_table.items():
            # Remove T from the RL set if it is present
            row['RL'].discard(T.name)
            # Reset the WL entry to None if T was holding WL
            if row['WL'] == T.name:
                row['WL'] = None
            # Remove T from the queue for locks on x
            new_wait_list = deque()
            for waiter in row['waiting']:
                if waiter[0] != T.name:
                    new_wait_list.append(waiter)
            row['waiting'] = new_wait_list
        
        # Finally remove node from waits_for graph
        if T.name in self.waits_for.nodes():
            self.waits_for.remove_node(T.name)