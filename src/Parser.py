import re
from src.request_response import Request
from src.Transaction import ReadWriteTransaction,ReadOnlyTransaction

class Parser(object):
    """Parser for the incoming operation requests.

    Attributes
    ----------
    ALL_COMMANDS : dict of regex : parse function
        Maps regex patterns, matching valid operations, to parsing functions.
    """

    def __init__(self):
        self.ALL_COMMANDS = {
            r'begin\((T[\d]+)\)': self.parse_begin,
            r'beginRO\((T[\d]+)\)': self.parse_beginRO,
            r'R\((T[\d]+)\s*,\s*(x[\d]+)\s*\)': self.parse_R,
            r'W\((T[\d]+)\s*,\s*(x[\d]+)\s*,\s*(.+)\s*\)': self.parse_W,
            r'dump\(\)': self.parse_dump,
            r'end\((T[\d]+)\)': self.parse_end,
            r'fail\(([\d]+)\)': self.parse_fail,
            r'recover\(([\d]+)\)': self.parse_recover
        }

    def success_callback(self,request,time):
        """Callback for requests which always succeed.
        """
        # Create and return dummy request
        request = Request(transaction=None,x=None,v=None,operation='success',
                          success=True,callback=lambda request,time: None)

        return request

    def parse_begin(self,m):
        """Parse begin transaction request, by creating a
        new ReadWriteTransaction.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to begin() pattern

        Side effects
        ------------
        Add read write transaction to self.transactions
        """
        T = m.group(1)
        self.transactions[T] = ReadWriteTransaction(T,self.time)
        
        # Create and return dummy request
        request = Request(transaction=self.transactions[T],
                          x=None,v=None,operation='begin',
                          success=True,callback=self.success_callback)

        return request
        
    def parse_beginRO(self,m):
        """Parse begin transaction request, by creating a
        new ReadOnlyTransaction.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to beginRO() pattern

        Side effects
        ------------
        Add read only transaction to self.transactions
        """
        T = m.group(1)
        self.transactions[T] = ReadOnlyTransaction(T,self.time)
        
        # Create and return dummy request
        request = Request(transaction=self.transactions[T],
                          x=None,v=None,operation='beginRO',
                          success=True,callback=self.success_callback)

        return request
        
    def parse_R(self,m):
        """ Transaction T tries to read variable x. 
        If T is read-only, then it must read x from
        the site with the most-recent commit to x, that
        is still alive.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to read() pattern

        See Also
        --------
        try_reading
        """
        T = self.transactions[m.group(1)]
        x = m.group(2)

        if T.read_only:
            request = Request(transaction=T,x=x,v=None,operation='R',
                            success=False,callback=self.manage_read_only_read_request)
        else:
            request = Request(transaction=T,x=x,v=None,operation='R',
                            success=False,callback=self.manage_read_write_read_request)
        return request
        
    def parse_W(self,m):
        """ Transaction T tries to write to variable x. 
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to write() pattern

        See Also
        --------
        try_writing
        """
        T = self.transactions[m.group(1)]
        x = m.group(2)
        v = m.group(3)

        # Create and return request
        request = Request(transaction=T,x=x,v=v,operation='W',
                          success=False,callback=self.manage_write_request)
        
        return request
        
    def parse_dump(self,m):
        """ Dump the current values at each site. 
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to dump() pattern
        """
        for site in range(1,11):
            kv = [(k,x.value) for k,x in self.sites[site].memory.items()]
            kv = sorted(kv,key=lambda x: int(x[0][1:]))
            print_str = f'site {site} -'
            for x in kv:
                print_str = print_str + f' {x[0]}: {x[1]},'
            print(print_str[:-1])
        
        # Create and return dummy request
        request = Request(transaction=None,
                          x=None,v=None,operation='dump',
                          success=True,callback=self.success_callback)

        return request
        
    def parse_end(self,m):
        """ End transaction T, either committing or aborting.
        If transaction T is read only, then it always commits.
        Otherwise we validate the commit request. If valid, 
        we commit; if not, we abort due to failure.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to end() pattern
        """
        T = self.transactions[m.group(1)]
        
        def end_callback(request,time):
            if T.read_only == True:
                # Then committing is trivial
                print(f"{T.name} commits")
                # Delete this transaction
                del self.transactions[T.name]
                
            else:
                # Then we need to check if T can commit
                if T.can_commit():
                    self.commit_transaction(T)
                else:
                    self.abort_transaction(T,'failure')

            return self.success_callback(request,self.time)
        
        # Create and return dummy request
        request = Request(transaction=None,
                          x=None,v=None,operation='end',
                          success=True,callback=end_callback)

        return request
        
    def parse_fail(self,m):
        """ Simulate site S failing. Note this also
        simulates the TM being alerted that the site has failed,
        and telling all transactions to drop locks at that site
        (and stop waiting on locks from the site).
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to fail() pattern
        
        See Also
        --------
        SiteManager.fail
        """
        
        def fail_callback(request,time):
            S = self.sites[int(m.group(1))]
            S.fail()

            # Tell transactions to remove the locks they're holding at thise site
            for T in self.transactions.values():
                T.drop_locks_at_dead_sites([S])
                
            return self.success_callback(request,self.time)
        
        # Create and return dummy request
        request = Request(transaction=None,
                          x=None,v=None,operation='fail',
                          success=True,callback=fail_callback)

        return request

    def parse_recover(self,m):
        """ Simulate site S recovering.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to recover() pattern
        
        See Also
        --------
        SiteManager.recover
        """
        def recover_callback(request,time):
            S = self.sites[int(m.group(1))]
            S.recover(self.time)
            return self.success_callback(request,self.time)
        
        # Create and return dummy request
        request = Request(transaction=None,
                          x=None,v=None,operation='recover',
                          success=True,callback=recover_callback)

        return request

    def parse_line(self,line):
        """ Parses a line of instruction from the input file/stdin,
        finding the matching pattern and dispatching to the associated
        parsing function.

        Parameters
        ----------
        line : str
            A line from the input operation requests (file or stdin)
        
        Raises
        ------
        ValueError
            If the line is invalid, raises a ValueError
        """
        matched = False
        for regex, p in self.ALL_COMMANDS.items():
            m = re.match(regex,line)
            if m is not None:
                return p(m)
        raise ValueError('Invalid command')