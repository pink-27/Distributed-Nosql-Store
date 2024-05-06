from abc import ABC, abstractmethod

class server (ABC):
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def _write_to_log(self, subject, predicate, new_object):
        pass

    @abstractmethod
    def _update_log_shard (self, subject, predicate, new_object):
        pass

    @abstractmethod
    def query(self, subject):
        pass
    
    @abstractmethod
    def update(self, subject, predicate, new_object):
        pass
    
    @abstractmethod
    def merge (self, server_name) :
        pass

    @abstractmethod
    def recover (self) :
        pass

    @abstractmethod
    def disconnect(self):
        pass

