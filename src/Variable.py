class Variable(object):
    """Stores state of variable

    Attributes
    ----------
    name : str
        Variable name (ex: x1)
    value : any
        Value stored in variable. Initialized to 10*x, where x
        is the variable index number
    replicated : bool
        True if the variable is replicated; else False
    available : bool
        True if the variable is readable. Initalized to readable,
        and becomes readable after failure only on write.
    """
    
    def __init__(self,var_num,replicated):
        self.name = f'x{var_num}'
        self.value = 10*var_num
        self.replicated = replicated
        self.available = True
    
    def read(self):
        """ Returns self.value. Caller's responsibility to check locks.
        
        Returns
        -------
        any:
            Value of variable
        """
        return self.value
    
    def write(self,v):
        """ Updates value, and sets the variable to available. Caller's
        responsibility to check locks.
        
        Parameters
        ----------
        v : any
            New value
        """
        self.value = v
        self.available = True