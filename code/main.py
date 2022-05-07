import os

from time import sleep

from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

# Declaring global variables
k8s_api_instance = None

# Declaring global const
ENTER_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --on'
EXIT_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --off'

def init_kubernetes_api():
    # Load and set KUBECONFIG
    config.load_kube_config()
    try:
        c = Configuration().get_default_copy()
    except AttributeError:
        c = Configuration()
        c.assert_hostname = False
    Configuration.set_default(c)

    # Create core api object
    global k8s_api_instance
    k8s_api_instance = core_v1_api.CoreV1Api()


def get_pod_by_label(namespace, app_name):
    resp = None
    selector="app="+app_name

    # Retrieving pod and checking that there is just one pod having that lable
    try:
        resp = k8s_api_instance.list_namespaced_pod(namespace=namespace, label_selector=selector)
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
            exit(1)
    
    if (resp is None) or (len(resp.items) != 1):
        print("There are too many pods with the label " + app_name + " or there isn't any. Just one pod must be present.")
        exit(3)
    else:
        return resp.items.pop()


def exec_container_command(namespace, app_name, command):
    # Retrieving the pod and checking that is in "Running" state
    pod = get_pod_by_label(namespace=namespace, app_name=app_name)
    if pod.status.phase != 'Running':
        print("The pod with label " + app_name + " is not in 'Running' state.")
        exit(4)
    
    # Creating exec command
    exec_command = ['/bin/bash', '-c', command]

    # Calling exec and waiting for response
    resp = stream(k8s_api_instance.connect_get_namespaced_pod_exec,
                  pod.metadata.name,
                  namespace,
                  command=exec_command,
                  stderr=True, stdin=False,
                  stdout=True, tty=False)
    return resp
    

def enter_maintenance_mode_nextcloud(namespace, app_name):
    resp = exec_container_command(namespace=namespace,app_name=app_name, command=ENTER_MAINTENANCE_CMD)
    if resp == "Maintenance mode enabled\n":
        sleep(30)
    else:
        print("Error entering maintenance mode")
        exit_maintenance_mode_nextcloud(namespace=namespace,app_name=app_name)
        exit(4)

def exit_maintenance_mode_nextcloud(namespace, app_name):
    resp = exec_container_command(namespace=namespace,app_name=app_name, command=EXIT_MAINTENANCE_CMD)
    if resp != "Maintenance mode disabled\n":
        print("Error exiting maintenance mode. You have to do it by hands")
        exit(5)
        
def create_mysql_backup():
    pass

def create_snapshots():
    pass

def create_backups():
    pass

def main():
    # Read ancd check the environment variables
    backup_type=os.getenv('BACK_TYPE')
    if backup_type not in ['FULL-BACKUP', 'SNAPSHOT']:
        print('Wrong backup type. BACK_TYPE is mandatory and must be either "FULL_BACKUP" or "SNAPSHOT"')
        exit(1)

    namespace=os.getenv('NAMESPACE')
    if namespace == None:
        namespace = 'default'

    app_name=os.getenv('APP_NAME')
    if app_name == None:
        print('"APP_NAME" environment variable is mandatory')
        exit(2)

    longhorn_url=os.getenv('LONGHORN_URL')
    if longhorn_url == None:
        print('The LONGHORN_URL env variable is mandatory.')
        exit(6)

    init_kubernetes_api()

    enter_maintenance_mode_nextcloud(namespace=namespace,app_name=app_name)

    exit_maintenance_mode_nextcloud(namespace=namespace,app_name=app_name)
    
    return 0


if __name__ == '__main__':
    main()
    exit(0)
