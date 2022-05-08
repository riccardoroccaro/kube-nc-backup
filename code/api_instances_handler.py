from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api

import longhorn

from lh_backup_exception import LHBackupException


class ApiInstancesHandlerException(LHBackupException):
    def __init__(self,message):
        super().__init__(message)


def init_kubernetes_api():
    # Load and set KUBECONFIG
    config.load_kube_config()
    try:
        c = Configuration().get_default_copy()
    except AttributeError:
        c = Configuration()
        c.assert_hostname = False
    Configuration.set_default(c)

    return core_v1_api.CoreV1Api()


def init_mariadb_client():


#TODO Implementare l'init


    pass


class ApiInstancesHandler:
    def __init__(self, longhorn_url):
        try:
            self.k8s_api_instance = init_kubernetes_api()
            self.lh_client = longhorn.Client(url=longhorn_url)
            self.mariadb_client = init_mariadb_client()
        except BaseException:
            if self.k8s_api_instance == None:
                del(self.k8s_api_instance)
                raise ApiInstancesHandlerException(message="Error creating Kubernetes API client instance")
            if self.lh_client == None:
                del(self.k8s_api_instance)
                del(self.lh_client)
                raise ApiInstancesHandlerException(message="Error creating Longhorn API client instance")
            if self.mariadb_client == None:
                del(self.k8s_api_instance)
                del(self.lh_client)
                del(self.mariadb_client)
                raise ApiInstancesHandlerException(message="Error creating MariaDB API client instance")


    def __del__(self):
        del(self.k8s_api_instance)
        del(self.lh_client)
        del(self.mariadb_client)
    
    ### k8s_api_instance getter, setter and deleter ###
    @property
    def k8s_api_instance(self):
        return self._k8s_api_instance
    
    @k8s_api_instance.setter
    def k8s_api_instance(self, k8s_api_instance):
        self._k8s_api_instance=k8s_api_instance

    @k8s_api_instance.deleter
    def k8s_api_instance(self):
        self.k8s_api_instance = None
    ### END ###

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
    