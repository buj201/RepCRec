from collections import defaultdict,namedtuple
from src.request_response import Request
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
    def drop_locks_at_dead_sites(self,dead_sites):
        pass

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
    blocked_request : None or Request
        If this transaction is blocked, store the next request
    locks_needed : dict of SiteManager:set
        Locks needed by the transaction, at each site, in order for
        blocked_request to proceed.
    """
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = False
        
        # Locks that this transaction is holding
        self.read_locks = defaultdict(set)
        self.write_locks = defaultdict(set)
        
        # Store state for blocked request. If lock-blocked, then request can 
        # only proceed when it has been given all the locks it needs
        self.blocked_request = None
        self.locks_needed = defaultdict(set)
        
        # Sites accessed by this transaction
        self.first_accessed = dict()
        
        # After image for writes by this transaction
        self.after_image = defaultdict(dict)
    
    def waiting_on_locks(self):
        """Check if the transaction is waiting on locks.
        If so, then the request has already been routed
        to a live site, and we can simply check if we've been
        given the required locks by calling `try_again`.

        Returns
        -------
        Bool
        """
        b = len(self.locks_needed)>0
        return b

    def update_first_accessed(self,site,time):
        """Utility to update first accessed. If this
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
        """Read site.x from the after_image, if this transaction
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
        """Write to the transaction's after-image of site.x
        
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
        """Main callback for re-trying operations blocked due to
        lock conflicts. Checks if the holding all required locks,
        and, if so, proceeds with the blocked operation.
        
        Parameters
        ----------
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
        # Check if we're holding all required locks
        has_all_needed_locks = True
        x = self.blocked_request.x
        for site in self.locks_needed:
            if (self.blocked_request.operation=='W') and (x not in self.write_locks[site]):
                has_all_needed_locks = False
            if (self.blocked_request.operation=='R') and (x not in self.read_locks[site]):
                has_all_needed_locks = False

        # If we have all required locks, and we're writing, then we can write
        if (self.blocked_request.operation=='W') and has_all_needed_locks:
            v = self.blocked_request.v
            for site in self.locks_needed:
                self.write(site,x,self.blocked_request.v,time)
            self.locks_needed = defaultdict(set)
            self.blocked_request = None
            return Request(transaction=self,x=x,v=v,
                            operation='W',success=True,callback=None)
        elif (self.blocked_request.operation=='W') and not has_all_needed_locks:
            return Request(transaction=self,x=x,v=self.blocked_request.v,
                           operation='W',success=False,callback=self.try_again)
        
        # If we have all required locks, and we're reading, proceed
        elif (self.blocked_request.operation=='R') and has_all_needed_locks:
            for site in self.locks_needed:
                # Should only execute once.
                v = self.read(site,x,time)
            self.locks_needed = defaultdict(set)
            self.blocked_request = None
            return Request(transaction=self,x=x,v=v,
                            operation='R',success=True,callback=None)
        else:
            return Request(transaction=self,x=x,v=None,
                            operation='R',success=False,callback=self.try_again)

    def can_commit(self):
        """Check if this transaction can commit (on end request), by
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

    def drop_locks_at_dead_sites(self,dead_sites):
        """Delete locks held at dead sites, and also remove these
        sites from the locks needed list (e.g. the sites at which
        this transaction is waiting for a lock).

        Parameters
        ----------
        dead_sites : List of SiteManagers
            Dead sites
        
        Returns
        -------
        None
        """
        for site in dead_sites:
            self.locks_needed[site] = set()
            self.write_locks[site] = set()
            self.read_locks[site] = set()
