class Variable(object):
    
    def __init__(self,var_num,replicated):
        self.name = f'x{var_num}'
        self.value = 10*var_num
        self.replicated = replicated
        self.available = True
    
    def read(self):
        """
        Returns self.value. Caller's responsibility to check locks.
        
        Args:
            None
            
        Return:
            self.value
            
        Side effects:
            None
        """
        return self.value
    
    def write(self,v):
        """
        Updates self.value. Caller's responsibility to check locks.
        
        Args:
            v: new value
        
        Returns:
            None
            
        Side effects:
            - Updates self.value = v
            - Updates self.available to True
        """
        self.value = v
        self.available = True