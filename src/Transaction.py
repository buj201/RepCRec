from collections import defaultdict,namedtuple
from src.nts import ReadResponse, WriteResponse, WriteOp, ReadOp
from copy import deepcopy

class Transaction(object):
    """Base class for Transactions.

    Attributes
    ----------
    start_time : int
        Start time for this transaction
    name : str
        Transaction name, e.g. 'T1'
    """

    def __init__(self,name,start_time):
        self.start_time = start_time
        self.name = name


class ReadOnlyTransaction(Transaction):
    """Read only transaction.
    """
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = True
        

class ReadWriteTransaction(Transaction):
    """Read write transaction, which obtains locks
    through two phase locking.

    Attributes
    ----------
    read_only : False
    read_locks : dict of SiteManager:set
        Set of read locks currently held by transaction at each site
    write_locks : dict of SiteManager:set
        Set of write locks currently held by transaction at each site
    first_accessed : dict of SiteManager:int
        Time that each site was first accessed (i.e. for read or write)
        by this transaction
    after_image : dict of dicts, mapping SiteManagers to variable:value pairs
        After image for writes to each site by this transaction
    next_op : None, WriteOp, or ReadOp
        If this transaction is blocked due to lock conflicts, stores details
        regarding the blocked request (variable, value for write requests)
    locks_needed : dict of SiteManager:set
        Locks needed by the transaction, at each site, in order for the blocked
        next_op to proceed.
    """
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = False
        
        # Locks that this transaction is holding
        self.read_locks = defaultdict(set)
        self.write_locks = defaultdict(set)
        
        # Sites accessed by this transaction
        self.first_accessed = dict()
        
        # After image for writes by this transaction
        self.after_image = defaultdict(dict)
        
        # Store state for blocked operations -- the transaction can only
        # proceed when it has been given all the locks it needs
        self.next_op = None
        self.locks_needed = defaultdict(set)
    
    def update_first_accessed(self,site,time):
        """ Utility to update first accessed. If this
        is the first time the transaction is accessing
        the site, then we set first_accessed for this site to time.
        Otherwise we don't update.
        
        Parameters
        ----------
        site : SiteManager
            Site being accessed
        time : Int
            Current time
        """
        if site not in self.first_accessed:
            self.first_accessed[site] = time
        
    
    def read(self,site,x,time):
        """ Read site.x from the after_image, if this transaction
        has previously written to site.x. Otherwise read
        directly from the sites memory.
        
        Parameters
        ----------
        site : SiteManager
            The site at which to read x
        x : Variable name
            The variable to read
        time : int
            Current time (to track first access times for each site)
            
        Returns
        -------
        Any:
            The transaction's view on site.x
        """
        
        self.update_first_accessed(site,time)
        
        if x in self.after_image[site]:
            return self.after_image[site][x]
        else:
            return site.read_from_memory(x)
        
    def write(self,site,x,v,time):
        """ Write to the transaction's after-image of site.x
        
        Parameters
        ----------
        site : SiteManager
            The site at which to read x
        x : Variable name
            The variable to read
        v : any
            The value to write to x
        time : int
            Current time (to track first access times for each site)
        """
        self.update_first_accessed(site,time)
        self.after_image[site][x] = deepcopy(v)
    
    def try_again(self,time):
        """ Main callback for re-trying operations blocked due to
        lock conflicts. Checks if the holding all required locks,
        and, if so, proceeds with the blocked operation.
        
        Parameters
        ----------
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.
            
        Returns
        -------
        Response
            ReadResponse if next_op is a ReadOp, or WriteResponse if
            next_op is a WriteOp
        """
        
        if isinstance(self.next_op,WriteOp):
            has_all_needed_locks = True
            
            # Find the variable we want to write
            x = self.next_op.x
            
            # See if we're now holding locks on x at all sites needed
            for site in self.locks_needed:
                if x not in self.write_locks[site]:
                    has_all_needed_locks = False
                    
            # If so, then we can write and unblock the transaction
            if has_all_needed_locks:
                for site in self.locks_needed:
                    self.write(site,x,self.next_op.v,time)
                self.locks_needed = defaultdict(set)
                self.next_op = None
                return WriteResponse(success=True,callback=None,transaction=self)
            
            # Otherwise we'll have to keep waiting
            else:
                return WriteResponse(success=False,callback=self.try_again,transaction=self)
                    
        elif isinstance(self.next_op,ReadOp):
            has_all_needed_locks = True
            
            # Find the variable we want to write
            x = self.next_op.x
            
            # See if we're now holding locks on x at target site
            for site in self.locks_needed:
                if x not in self.read_locks[site]:
                    has_all_needed_locks = False
                    
            # If so read x
            if has_all_needed_locks:
                for site in self.locks_needed:
                    # Should only execute once.
                    v = self.read(site,x,time)
                self.locks_needed = defaultdict(set)
                self.next_op = None
                
                print(f'{self.name}: {x}={v}')
                return ReadResponse(success=True,callback=None,value=v,transaction=self)
            else:
                return ReadResponse(success=False,callback=self.try_again,value=None,transaction=self)

    def can_commit(self):
        """ Check if this transaction can commit (on end request), by
        verifying that all of the sites it accessed have been live since
        the transaction first accessed the site.
            
        Returns
        -------
        bool
            True if this transaction can commit, else False
        """
        all_sites_fully_alive = True
        for site in self.first_accessed:
            if (site.uptime >= self.first_accessed[site]) or (not site.alive):
                all_sites_fully_alive = (False and all_sites_fully_alive)
        return all_sites_fully_alive