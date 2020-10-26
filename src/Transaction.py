from collections import defaultdict,namedtuple
from src.request_response import RequestResponse
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
    locks_needed : dict of SiteManager:set
        Locks needed by the transaction, at each site, in order for
        its next queued request to proceed.
    first_accessed_time : dict of SiteManager:int
        Time that each site was first accessed (i.e. for read or write)
        by this transaction
    after_image : dict of dicts, mapping SiteManagers to variable:value pairs
        After image for writes to each site by this transaction
    """
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = False
        
        # Locks that this transaction is holding
        self.read_locks = defaultdict(set)
        self.write_locks = defaultdict(set)
        
        # Store state for blocked request. If lock-blocked, then request can 
        # only proceed when it has been given all the locks it needs
        self.locks_needed = defaultdict(set)
        
        # Sites accessed by this transaction
        self.first_accessed_time = dict()
        
        # After image for writes by this transaction
        self.after_image = defaultdict(dict)
    
    def is_waiting_on_locks(self):
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
    
    def is_holding_all_required_locks(self,request):
        """Checks if the transaction has all required locks to
        execute the request.

        Parameters
        ----------
        request : RequestResponse
            A "R" or "W" RequestResponse.

        Returns
        -------
        bool
            True if the transaction is holding all required locks
        """
        # Check if we're holding all required locks
        has_all_needed_locks = True
        x = request.x
        for site in self.locks_needed:
            if (request.operation=='W') and (x not in self.write_locks[site]):
                has_all_needed_locks = False
            if (request.operation=='R') and (x not in self.read_locks[site]):
                has_all_needed_locks = False
        return has_all_needed_locks

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
        for site in self.first_accessed_time:
            if (site.uptime >= self.first_accessed_time[site]) or (not site.alive):
                all_sites_fully_alive = (False and all_sites_fully_alive)
        return all_sites_fully_alive

    def update_first_accessed_time(self,site,time):
        """Utility to update first accessed. If this
        is the first time the transaction is accessing
        the site, then we set first_accessed_time for this site to time.
        Otherwise we don't update.
        
        Parameters
        ----------
        site : SiteManager
            Site being accessed
        time : Int
            Current time
        """
        if site not in self.first_accessed_time:
            self.first_accessed_time[site] = time
        
    def read_site_x(self,site,x,time):
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
        
        self.update_first_accessed_time(site,time)
        
        if x in self.after_image[site]:
            return self.after_image[site][x]
        else:
            return site.read_from_memory(x)
        
    def write_v_to_site_x(self,site,x,v,time):
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
        self.update_first_accessed_time(site,time)
        self.after_image[site][x] = deepcopy(v)

    def write_if_holding_all_required_locks(self,request,time):
        """Write to all available copies, if holding all required locks.
        Assumes caller confirmed the blocked request is a write request.
        
        Parameters
        ----------
        request : RequestResponse
            A "W" RequestResponse
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read/write requests.
            
        Returns
        -------
        nts.RequestResponse
        
        See Also
        --------
        nts.RequestResponse
        """
        v = request.v
        x = request.x

        has_all_needed_locks = self.is_holding_all_required_locks(request)

        # If we have all required locks, proceed
        if has_all_needed_locks:
            for site in self.locks_needed:
                self.write_v_to_site_x(site,x,v,time)
            self.locks_needed = defaultdict(set)

            return RequestResponse(transaction=self,x=x,v=v,
                            operation='W',success=True,
                            callback=None)
        else:
            return RequestResponse(transaction=self,x=x,v=v,
                           operation='W',success=False,
                           callback=request.callback)
    
    def read_if_holding_all_required_locks(self,request,time):
        """Write to all available copies, if holding all required locks.
        Assumes caller confirmed the blocked request is a read request.
        
        Parameters
        ----------
        request : RequestResponse
            A "R" RequestResponse
        time : int
            Time at which this callback is re-executed, so the
            access time can be stored for successful read requests.
            
        Returns
        -------
        nts.RequestResponse
        
        See Also
        --------
        nts.RequestResponse
        """
        v = request.v
        x = request.x

        has_all_needed_locks = self.is_holding_all_required_locks(request)
        
        # If we have all required locks, proceed
        if has_all_needed_locks:
            for site in self.locks_needed:
                v = self.read_site_x(site,x,time)
            self.locks_needed = defaultdict(set)
            
            return RequestResponse(transaction=self,x=x,v=v,
                            operation='R',success=True,
                            callback=None)
        else:
            return RequestResponse(transaction=self,x=x,v=None,
                            operation='R',success=False,
                            callback=request.callback)

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
            if site in self.locks_needed:
                self.locks_needed.pop(site)
            if site in self.write_locks:
                self.write_locks.pop(site)
            if site in self.read_locks:
                self.read_locks.pop(site)
