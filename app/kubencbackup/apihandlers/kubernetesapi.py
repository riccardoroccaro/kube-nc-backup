import os
import functools

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

### Methods decorator ###
def exec_only_if_conn_init(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.is_connection_initialized:
            method(self, *args, **kwargs)
        else:
            self.log_err("Connection to Kubernetes API not initialized")
            raise K8sApiInstanceHandlerException(message="Connection to Kubernetes API not initialized")
    return wrapper
### END ###

### Handler ###
class K8sApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)

class K8sApiInstanceHandler(Loggable):
    def __init__(self,conf):
        super().__init__(name="KUBERNETES-API", log_level=2)
        try:
            self.__is_connection_initialized = False
            self.conf=conf
        except K8sApiInstanceHandlerException as e:
            self.log_err("Error creating Kubernetes API client instance: " + str(e))
            raise K8sApiInstanceHandlerException(message="Error creating Kubernetes API client instance")
        except:
            self.log_err("Unknown error while creating Kubernetes API client instance")
            raise K8sApiInstanceHandlerException(message="Unknown error while creating Kubernetes API client instance")

        # Init in-cluster mode
        try:
            if os.getenv("IN_CLUSTER_MODE").lower() != "false":
                self.__in_cluster_mode = True
            else:
                self.__in_cluster_mode = False
        except:
            self.__in_cluster_mode = True
            
    def __enter__(self):
        self.log_info(msg="Initializing Kubernetes API...")
        try:
            # Load and set KUBECONFIG
            self.log_info(msg="Loading config...")
            if self.in_cluster_mode:
                config.load_incluster_config()
            else:
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
        except ApiException:
            self.log_err(err="Error creating Kubernetes API client instance")
            self.free_resources()
            raise K8sApiInstanceHandlerException(message="Error creating Kubernetes API client instance")
        except:
            self.log_err(err="Unknown error while creating Kubernetes API client instance")
            self.free_resources()
            raise K8sApiInstanceHandlerException(message="Unknown error while creating Kubernetes API client instance")

        self.__is_connection_initialized = True
        
        return self

    def __exit__(self, *a):
        self.free_resources()

    def __del__(self):
        self.free_resources()

    def free_resources(self):
        try:
            del(self.k8s_api_instance)
            self.log_info(msg="Kubernetes API resources succesfully cleaned up")
            self.__is_connection_initialized = False
        except (AttributeError,NameError):
            pass
        except:
            self.log_err(err="Unable to clean up the Kubernetes API resources")

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

    ### in_cluster_mode getter###
    @property
    def in_cluster_mode(self):
        return self.__in_cluster_mode
    ### END ###

    ### is_connection_initialized getter###
    @property
    def is_connection_initialized(self):
        return self.__is_connection_initialized
    ### END ###

    ### Methods implementation ###
    @exec_only_if_conn_init
    def get_pod_by_label(self, label):
        resp = None

        self.log_info(msg="Retrieving the pod by label and checking that there is just one pod having that label")
        # Retrieving pod and checking that there is just one pod having that label
        try:
            resp = self.k8s_api_instance.list_namespaced_pod(namespace=self.conf.namespace, label_selector=label)
        except ApiException as e:
            if e.status != 404:
                self.log_err(err="Unknown Kubernetes API error")
                raise K8sApiInstanceHandlerException(message="Unknown Kubernetes API error")
        except:
            self.log_err(err="Unknown error")
            raise K8sApiInstanceHandlerException(message="Unknown error")
        
        if (resp is None) or (len(resp.items) != 1):
            self.log_err(err="There are too many pods with the label " + label + " or there isn't any. Just one pod must be present.")
            raise K8sApiInstanceHandlerException(message="There are too many pods with the label " + label + " or there isn't any. Just one pod must be present.")
        else:
            return resp.items.pop()
    
    @exec_only_if_conn_init
    def get_pv_name_from_pvc_name(self,pvc_name):
        try:
            self.log_info("Retrieving the PV by PVC name " + pvc_name)
            pvc=self.k8s_api_instance.read_namespaced_persistent_volume_claim(namespace=self.conf.namespace, name=pvc_name)
        except ApiException:
            self.log_err(err="Kubernetes API error retrieving PV name by PVC name "+ pvc_name)
            raise K8sApiInstanceHandlerException(message="Kubernetes API error retrieving Persistent Volume name from Persistent Volume Claim "+ pvc_name)
        except:
            self.log_err(err="Unknown error while retrieving PV name by PVC name "+ pvc_name)
            raise K8sApiInstanceHandlerException(message="Unknown error while retrieving Persistent Volume name from Persistent Volume Claim "+ pvc_name)
        return pvc.spec.volume_name

    @exec_only_if_conn_init
    def exec_container_command(self, pod_label, command):
        # Retrieving the pod and checking that is in "Running" state
        self.log_info(msg="Retrieving the pod and checking it is in Running state....")
        pod = self.get_pod_by_label(label=pod_label)
        if pod.status.phase != 'Running':
            self.log_err(err="The pod with label " + pod_label + " is not in 'Running' state.")
            raise K8sApiInstanceHandlerException(message="The pod with label " + pod_label + " is not in 'Running' state.")
        
        # Creating exec command
        exec_command = ['/bin/bash', '-c', command]

        self.log_info(msg="DONE. Executing the command inside the pod...")
        # Calling exec and waiting for response
        try: 
            resp = stream(self.k8s_api_instance.connect_get_namespaced_pod_exec,
                        pod.metadata.name,
                        namespace=self.conf.namespace,
                        command=exec_command,
                        stderr=True, stdin=False,
                        stdout=True, tty=False)
        except ApiException as e:
            self.log_err(err="Kubernetes API error. Unable to execute the command")
            raise K8sApiInstanceHandlerException(message="Kubernetes API error. Unable to execute the command")
        except:
            self.log_err(err="Unknown error. Unable to execute the command")
            raise K8sApiInstanceHandlerException(message="Unknown error. Unable to execute the command")
        self.log_info(msg="Done. Command successfully executed")
        return resp
    ### END - Methods implementation ###
### END - Handler ###
