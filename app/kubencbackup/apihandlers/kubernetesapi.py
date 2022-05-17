from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from kubencbackup.common.backupconfig import BackupConfig
from kubencbackup.common.backupexceptions import ApiInstancesConfigException, ApiInstancesHandlerException
from kubencbackup.common.loggable import Loggable

### Config ###
class K8sApiInstanceConfigException(ApiInstancesConfigException):
    def __init__(self,message):
        super().__init__(message)


class K8sApiInstanceConfig:
    def __init__(self, namespace):
        self.namespace=namespace

    ### namespace getter and setter ###
    @property
    def namespace(self):
        return self.__namespace
    
    @namespace.setter
    def namespace(self,namespace):
        if namespace == None:
            self.__namespace = BackupConfig.DEFAULT_NAMESPACE
        else:
            self.__namespace=namespace
    ### END ###
        
### END - Config ###

### Handler ###
class K8sApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)

class K8sApiInstanceHandler(Loggable):
    def __init__(self,conf):
        super().__init__(name="KUBERNETES-API")
        try:
            self.conf=conf
        except BaseException:
            raise K8sApiInstanceHandlerException(message="Error creating Kubernetes API client instance")
            
    def __enter__(self):
        self.log_info(msg="Initializing Kubernetes API...")
        try:
            # Load and set KUBECONFIG
            self.log_info(msg="Loading config...")
            config.load_kube_config()
            try:
                c = Configuration().get_default_copy()
            except AttributeError:
                c = Configuration()
                c.assert_hostname = False
            Configuration.set_default(c)

            self.log_info(msg="DONE. Retrieving API endpoint...")
            self.k8s_api_instance = core_v1_api.CoreV1Api()

            self.log_info(msg="DONE. Kubernetes API successfully initialized.")
        except (BaseException,ApiException):
            self.log_err(err="Error creating Kubernetes API client instance")
            self.free_resources()
            raise K8sApiInstanceHandlerException(message="Error creating Kubernetes API client instance")
        
        return self

    def __exit__(self, *a):
        self.free_resources()

    def __del__(self):
        self.free_resources()

    def free_resources(self):
        if self.k8s_api_instance != None:
            self.log_info(msg="Cleaning the Kubernetes API resources")
            del(self.k8s_api_instance)

    ### k8s_api_instance getter, setter and deleter ###
    @property
    def k8s_api_instance(self):
        return self.__k8s_api_instance
    
    @k8s_api_instance.setter
    def k8s_api_instance(self, k8s_api_instance):
        self.__k8s_api_instance=k8s_api_instance

    @k8s_api_instance.deleter
    def k8s_api_instance(self):
        del(self.__k8s_api_instance)
        self.k8s_api_instance = None
    ### END - k8s_api_instance ###

    ### conf getter, setter and deleter ###
    @property
    def conf(self):
        return self.__conf
    
    @conf.setter
    def conf(self, conf):
        if conf == None or type(conf) != K8sApiInstanceConfig:
            self.log_err(err="the configuration object mustn't be None and must be a K8sApiInstanceConfig instance")
            raise K8sApiInstanceHandlerException(message="conf mustn't be None and must be a K8sApiInstanceConfig instance")
        self.__conf=conf
    ### END - conf ###


    ### Methods implementation ###
    def get_pod_by_label(self, label):
        resp = None

        self.log_info(msg="Retrieving the pod by label and checking that there is just one pod having that label")
        # Retrieving pod and checking that there is just one pod having that label
        try:
            resp = self.k8s_api_instance.list_namespaced_pod(namespace=self.conf.namespace, label_selector=label)
        except ApiException as e:
            if e.status != 404:
                raise K8sApiInstanceHandlerException(message="Unknown error: "+e)
        
        if (resp is None) or (len(resp.items) != 1):
            self.log_err(err="There are too many pods with the label " + label + " or there isn't any. Just one pod must be present.")
            raise K8sApiInstanceHandlerException(message="There are too many pods with the label " + label + " or there isn't any. Just one pod must be present.")
        else:
            return resp.items.pop()
    
    def get_pv_name_from_pvc_name(self,pvc_name):
        try:
            self.log_info("Retrieving the PV by PVC name " + pvc_name)
            pvc=self.k8s_api_instance.read_namespaced_persistent_volume_claim(namespace=self.conf.namespace, name=pvc_name)
        except BaseException:
            self.log_err(err="Error retrieving PV name by PVC name "+ pvc_name)
            raise K8sApiInstanceHandlerException(message="Error retrieving Persistent Volume name from Persistent Volume Claim "+ pvc_name)
        return pvc.spec.volume_name

    def exec_container_command(self, pod_label, command):
        # Retrieving the pod and checking that is in "Running" state
        pod = self.get_pod_by_label(label=pod_label)
        if pod.status.phase != 'Running':
            self.log_err(err="The pod with label " + pod_label + " is not in 'Running' state.")
            raise K8sApiInstanceHandlerException(message="The pod with label " + pod_label + " is not in 'Running' state.")
        
        # Creating exec command
        exec_command = ['/bin/bash', '-c', command]

        self.log_info(msg="Executing the command "+ exec_command + " inside the pod...")
        # Calling exec and waiting for response
        try: 
            resp = stream(self.k8s_api_instance.connect_get_namespaced_pod_exec,
                        pod.metadata.name,
                        namespace=self.conf.namespace,
                        command=exec_command,
                        stderr=True, stdin=False,
                        stdout=True, tty=False)
        except BaseException as e:
            self.log_err(err="Unable to execute the command " + exec_command )
            raise K8sApiInstanceHandlerException(message="Unable to execute the command " + command +  ". The call returned this message:\n" + e)
        return resp
    ### END - Methods implementation ###

### END - Handler ###
