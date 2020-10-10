import re

from src.Transaction import Transaction,ReadOnlyTransaction

class Parser(object):

    def __init__(self):
        
        self.ALL_COMMANDS = {
            r'begin\((T[\d]+)\)': self.p_BEGIN,
            r'beginRO\((T[\d]+)\)': self.p_BEGIN_RO,
            r'R\((T[\d]+),(x[\d]+)\)': self.p_READ,
            r'W\((T[\d]+),(x[\d]+),(.+)\)': self.p_WRITE,
            r'dump\(\)': self.p_DUMP,
            r'end\((T[\d]+)\)': self.p_END,
            r'fail\(([\d]+)\)': self.p_FAIL,
            r'recover\(([\d]+)\)': self.p_RECOVER
        }

    def p_BEGIN(self,m):
        """
        Begin transaction
        
        Args:
            m: re.match corresponding to begin() command
            
        Returns:
            None
            
        Side effects:
            Add transaction to self.transactions
        """
        T = m.group(1)
        self.transactions[T] = Transaction(T,self.time)
        
    def p_BEGIN_RO(self,m):
        """
        Begin read-only transaction
        
        Args:
            m: re.match corresponding to begin() command
            
        Returns:
            None
            
        Side effects:
            Add read only transaction to self.transactions
        """
        T = m.group(1)
        self.transactions[T] = ReadOnlyTransaction(T,self.time)
        
    def p_READ(self,m):
        """
        Transaction T tries to read variable x. 
        If T is read-only, then it must read x from:
            - A site that is up
            - And has the most-recent commit to x
        
        Args:
            m: re.match corresponding to read() command
            
        Returns:
            str: string formatted as '{x}: {x value}'
            
        Side effects:
            
        
        """
        T = self.transactions[m.group(1)]
        x = m.group(2)
        self.try_reading(T,x)
        
    def p_WRITE(self,m):
        T = self.transactions[m.group(1)]
        x = m.group(2)
        v = m.group(3)
        self.try_writing(T,x,v)
        
    def p_DUMP(self,m):
        for site in range(1,11):
            kv = [(k,x.value) for k,x in self.sites[site].memory.items()]
            kv = sorted(kv,key=lambda x: int(x[0][1:]))
            print_str = f'site {site} -'
            for x in kv:
                print_str = print_str + f' {x[0]}: {x[1]},'
            print(print_str[:-1])
        
    def p_END(self,m):
        T = self.transactions[m.group(1)]
        
        
        # Scrub T from the lock tables
        for site_number,site in self.sites.items():
            site.scrub_transaction_from_table(T)
            
        # TODO -- commit...
        
    def p_FAIL(self,m):
        S = m.group(1)
        return f'Site {S} failing'
        
    def p_RECOVER(self,m):
        S = m.group(1)
        return f'Site {S} recovering'
        
    def p_line(self,line):
        """
        Parses a line of instruction from the input file/stdin
        """
        matched = False
        for regex, p in self.ALL_COMMANDS.items():
            m = re.match(regex,line)
            if m is not None:
                return p(m)
        raise ValueError('Invalid command')