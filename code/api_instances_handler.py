from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api

import longhorn

class ApiInstancesHandler:
    def __init__(self, longhorn_url):
        try:
            self.lh_client = longhorn.Client(url=longhorn_url)
        except BaseException:
            if self.lh_client == None:
                del(self.k8s_api_instance)
                del(self.lh_client)
                raise ApiInstanceHandlerException(message="Error creating Longhorn API client instance")

    def __del__(self):
        del(self.lh_client)

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
    