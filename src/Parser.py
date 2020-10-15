import re
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
            r'begin\((T[\d]+)\)': self.p_BEGIN,
            r'beginRO\((T[\d]+)\)': self.p_BEGIN_RO,
            r'R\((T[\d]+)\s*,\s*(x[\d]+)\s*\)': self.p_READ,
            r'W\((T[\d]+)\s*,\s*(x[\d]+)\s*,\s*(.+)\s*\)': self.p_WRITE,
            r'dump\(\)': self.p_DUMP,
            r'end\((T[\d]+)\)': self.p_END,
            r'fail\(([\d]+)\)': self.p_FAIL,
            r'recover\(([\d]+)\)': self.p_RECOVER
        }

    def p_BEGIN(self,m):
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
        
    def p_BEGIN_RO(self,m):
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
        
    def p_READ(self,m):
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
        self.try_reading(T,x)
        
    def p_WRITE(self,m):
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
        self.try_writing(T,x,v)
        
    def p_DUMP(self,m):
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
        
    def p_END(self,m):
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
        
        if T.read_only == True:
            # Then committing is trivial
            print(f"{T.name} commits")
            
        else:
            # Then we need to check if T can commit
            if T.can_commit():
                self.commit(T)
            else:
                self.abort(T,'failure')
        
    def p_FAIL(self,m):
        """ Simulate site S failing.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to fail() pattern
        
        See Also
        --------
        SiteManager.fail
        """
        S = self.sites[int(m.group(1))]
        S.fail()
        
    def p_RECOVER(self,m):
        """ Simulate site S recovering.
        
        Parameters
        ----------
        m : re.match
            Match object corresponding to recover() pattern
        
        See Also
        --------
        SiteManager.recover
        """
        S = self.sites[int(m.group(1))]
        S.recover(self.time)
        
    def p_line(self,line):
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