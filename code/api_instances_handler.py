from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api

import longhorn

def init_mariadb_client():
#TODO Implementare l'init
    pass

class ApiInstancesHandler:
    def __init__(self, longhorn_url):
        try:
            self.lh_client = longhorn.Client(url=longhorn_url)
            self.mariadb_client = init_mariadb_client()
        except BaseException:
            if self.lh_client == None:
                del(self.k8s_api_instance)
                del(self.lh_client)
                raise ApiInstanceHandlerException(message="Error creating Longhorn API client instance")
            if self.mariadb_client == None:
                del(self.k8s_api_instance)
                del(self.lh_client)
                del(self.mariadb_client)
                raise ApiInstanceHandlerException(message="Error creating MariaDB API client instance")


    def __del__(self):
        del(self.lh_client)
        del(self.mariadb_client)

    ### lh_client getter, setter and deleter ###
    @property
    def lh_client(self):
        return self._lh_client
    
    @lh_client.setter
    def lh_client(self, lh_client):
        self._lh_client=lh_client

    @lh_client.deleter
    def lh_client(self):
        self.lh_client = None
    ### END ###

    ### mariadb_client getter, setter and deleter ###
    @property
    def mariadb_client(self):
        return self._mariadb_client
    
    @mariadb_client.setter
    def mariadb_client(self, mariadb_client):
        self._mariadb_client=mariadb_client

    @mariadb_client.deleter
    def mariadb_client(self):


#TODO Chiudere la connessione!!!


        self.mariadb_client = None
    ### END ###
    