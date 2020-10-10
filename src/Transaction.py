class Transaction(object):
    def __init__(self,name,start_time):
        self.start_time = start_time
        self.name = name
        self.read_only = False
        self.read_locks = {site:set() for site in range(1,21)}
        self.write_locks = {site:set() for site in range(1,21)}
        self.sites_accessed = set()
        
    def give_up_write_lock(self,site,x):
        site.release_WL(T,x)
        self.write_locks[site].remove(x)
    
    def give_up_read_lock(self,site,x):
        site.release_RL(T,x)
        self.read_locks[site].remove(x)


class ReadOnlyTransaction(Transaction):
    def __init__(self,name,start_time):
        super().__init__(name,start_time)
        self.read_only = True
    