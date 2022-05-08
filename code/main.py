import os
from datetime import datetime
from time import sleep

from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

import longhorn

# Declaring global variables
k8s_api_instance = None
lh_client = None

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


def init_longhorn_client(longhorn_url):
    global lh_client
    lh_client = longhorn.Client(url=longhorn_url)


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


def get_pv_name_by_pvc_name(namespace,pvc_name):
    global k8s_api_instance
    pvc=k8s_api_instance.read_namespaced_persistent_volume_claim(namespace=namespace, name=pvc_name)
    return pvc.spec.volume_name

#TODO Add backup file path
def create_mysqldump_backup(namespace,db_name,password):
    command = "mysqldump --add-drop-database --add-drop-table --lock-all-tables --result-file=backup.sql --password="+password+" --all-databases"
    exec_container_command(namespace=namespace,app_name=db_name,command=command)


def create_volume_backup_or_snapshot(backup_type, backup_name_suffix, volume_name, namespace, app_name):
    # Retrieve the volume to backup/snapshot
    global client
    volume = client.by_id_volume(id=get_pv_name_by_pvc_name(namespace=namespace,pvc_name=volume_name))

    # Create the snapshot
    snapshot=volume.snapshotCreate(name=volume_name+"__"+backup_name_suffix)

    # Create backup from snapshot if required
    if backup_type == 'FULL-BACKUP':
        # Create the backup
        volume.snapshotBackup(name=snapshot.name)
    else:
        print("Unexpected error.")
        exit_maintenance_mode_nextcloud(namespace=namespace,app_name=app_name)
        exit(7)


def create_mariadb_volume_backup_or_snapshot():
    # Init MariaDB client

    # Block databases for backup

    # Create the snapshot/backup

    # Unblock databases after backup

    # Close MariaDB connections
    pass


def clean_old_backup_snapshot():
    pass

def main():
    # Read ancd check the environment variables
    backup_type=os.getenv('BACKUP_TYPE')
    if backup_type not in ['FULL-BACKUP', 'SNAPSHOT']:
        print('Wrong backup type. BACKUP_TYPE is mandatory and must be either "FULL_BACKUP" or "SNAPSHOT"')
        exit(1)

    namespace=os.getenv('NAMESPACE')
    if namespace == None:
        namespace = 'default'

    app_name=os.getenv('APP_NAME')
    if app_name == None:
        print('"APP_NAME" environment variable is mandatory')
        exit(2)
    
    db_name=os.getenv('DB_APP_NAME')
    if db_name == None:
        print('"DB_APP_NAME" environment variable is mandatory')
        exit(8)

    db_password=os.getenv('DB_ROOT_PASSWORD')
    if db_password == None:
        print('"DB_ROOT_PASSWORD" environment variable is mandatory')
        exit(8)

    longhorn_url=os.getenv('LONGHORN_URL')
    if longhorn_url == None:
        print('The LONGHORN_URL env variable is mandatory.')
        exit(6)

    mysql_backup_volume_name=os.getenv('MYSQL_BACKUP_VOLUME_NAME')
    if mysql_backup_volume_name == None:
        print('The MYSQL_VOLUME_NAME env variable is mandatory.')
        exit(6)

    nextcloud_volume_name=os.getenv('NEXTCLOUD_VOLUME_NAME')
    if nextcloud_volume_name == None:
        print('The NEXTCLOUD_VOLUME_NAME env variable is mandatory.')
        exit(6)

    # Init kubernetes, longhorn and mariadb clients
    init_kubernetes_api()
    init_longhorn_client(longhorn_url=longhorn_url)

    ### Init backup process ###

    # Enter NextCloud Maintenance Mode
    enter_maintenance_mode_nextcloud(namespace=namespace,app_name=app_name)

    # Create MariaDB backup
    create_mysqldump_backup(namespace=namespace,db_name=db_name,password=db_password)

    # Create volumes backup suffix 
    backup_name_suffix=datetime.now().strftime("%d-%m-%Y__%H-%M-%S")

    ### Backup volumes (Nextcloud volume, actual MariaDB volume, backup MariaDB volume) ###

    # Create longhorn nextcloud snapshots and backup
    create_volume_backup_or_snapshot(backup_type=backup_type, backup_name_suffix=backup_name_suffix, volume_name=nextcloud_volume_name, namespace=namespace)

    # Create longhorn MariaDB actual volume snapshots and backup
    create_mariadb_volume_backup_or_snapshot()

    # Create longhorn MariaDB backup volume snapshots and backup
    create_volume_backup_or_snapshot(backup_type=backup_type, backup_name_suffix=backup_name_suffix, volume_name=mysql_backup_volume_name, namespace=namespace)

    # Exit NextCloud Maintenance Mode
    exit_maintenance_mode_nextcloud(namespace=namespace,app_name=app_name)

    # Clean old backup/snapshot
    clean_old_backup_snapshot()

    return 0


if __name__ == '__main__':
    main()
    exit(0)

# TODO Add try-except blocks for each critical third-party interaction to nicely release the resources