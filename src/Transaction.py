from collections import defaultdict,namedtuple
from src.nts import ReadResponse, WriteResponse, WriteOp, ReadOp
from copy import deepcopy

class Transaction(object):
     def __init__(self,name,start_time):
        self.start_time = start_time
        self.name = name

class ReadWriteTransaction(Transaction):
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = False
        
        # Locks that this transaction is holding
        self.read_locks = defaultdict(set)
        self.write_locks = defaultdict(set)
        
        # Sites accessed by this transaction
        self.accessed = set()
        
        # After image for writes by this transaction
        self.after_image = defaultdict(dict)
        
        # Store state for blocked operations -- the transaction can only
        # proceed when it has been given all the locks
        self.next_op = None
        self.locks_needed = defaultdict(set)
    
    def read(self,site,x):
        """
        Read site.x from the after_image, if it exists.
        Otherwise read from the site directly
        
        Args:
            site: the site to read
            x: the variable to read
            
        Returns:
            The transaction's view on site.x
            
        Side effects:
            None
        """
        
        self.accessed.add(site)
        
        if x in self.after_image[site]:
            return self.after_image[site][x]
        else:
            return site.read_from_memory(x)
        
    def write(self,site,x,v):
        """
        Write to the transaction's after-image of site.x
        
        Args:
            site: the site to write to
            x: the variable to write to
            v: the value to write to site.x
            
        Returns:
            None
            
        Side effects:
            None
        """
        self.accessed.add(site)
        self.after_image[site][x] = deepcopy(v)
    
    def try_again(self):
        """
        Try the blocked operation.
        Checks if the holding all required locks, and, if so,
        proceeds with the blocked operation.
        
        Args:
            None
            
        Returns:
            - ReadResponse if next_op is a ReadOp
            - WriteResponse if next_op is a WriteOp
            
        Side effects:
            Writes modify the transactions after-image.
        """
        
        if isinstance(self.next_op,WriteOp):
            has_all_needed_locks = True
            # Get the variable we need to write
            x = self.next_op.x
            
            for site in self.locks_needed:
                if x not in self.write_locks[site]:
                    has_all_needed_locks = False
            if has_all_needed_locks:
                for site in self.locks_needed:
                    self.write(site,x,self.next_op.v)
                self.locks_needed = defaultdict(set)
                self.next_op = None
                return WriteResponse(success=True,callback=None,transaction=self)
            else:
                return WriteResponse(success=False,callback=lambda: self.try_again(),transaction=self)
                    
        elif isinstance(self.next_op,ReadOp):
            has_all_needed_locks = True
            # Get the variable we need to read
            x = self.next_op.x
            
            for site in self.locks_needed:
                if x not in self.read_locks[site]:
                    has_all_needed_locks = False
            if has_all_needed_locks:
                for site in self.locks_needed:
                    # Should only execute once.
                    v = self.read(site,x)
                self.locks_needed = defaultdict(set)
                self.next_op = None
                return ReadResponse(success=True,callback=None,value=v,transaction=self)
            else:
                return ReadResponse(success=False,callback=self.try_again(),value=None,transaction=self)

    def can_commit(self):
        """
        This transaction can commit (on end request) if
        all of the sites it accessed have been live since
        the transaction started.
        
        Args:
            None
            
        Returns:
            bool: True if this transaction can commit
            
        Side effects:
            None
        """
        all_sites_fully_alive = True
        for site in self.accessed:
            if site.uptime > self.start_time:
                all_sites_fully_alive = (False and all_sites_fully_alive)
        return all_sites_fully_alive
            
class ReadOnlyTransaction(Transaction):
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = True
        