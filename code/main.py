from datetime import datetime
from time import sleep

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from api_instances_handler import ApiInstancesHandler
from lh_backup_env_handler import LHBackupEnvironmentException
from api_instances_handler import ApiInstancesHandlerException
from lh_backup_env_handler import LHBackupEnvironment


# Declaring global const
ENTER_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --on'
EXIT_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --off'

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
        exit(1)
    else:
        return resp.items.pop()


def exec_container_command(namespace, app_name, command):
    # Retrieving the pod and checking that is in "Running" state
    pod = get_pod_by_label(namespace=namespace, app_name=app_name)
    if pod.status.phase != 'Running':
        print("The pod with label " + app_name + " is not in 'Running' state.")
        exit(1)
    
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
        exit(1)


def exit_maintenance_mode_nextcloud(namespace, app_name):
    resp = exec_container_command(namespace=namespace,app_name=app_name, command=EXIT_MAINTENANCE_CMD)
    if resp != "Maintenance mode disabled\n":
        print("Error exiting maintenance mode. You have to do it by hands")
        exit(1)


def get_pv_name_by_pvc_name(namespace,pvc_name):
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
        exit(1)


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
    # Init Environment and kubernetes, longhorn and mariadb clients
    try:
        lhbe = LHBackupEnvironment()
        apis = ApiInstancesHandler(longhorn_url=lhbe.longhorn_url)
    except LHBackupEnvironmentException as ee:
        print(ee)
        exit(1)
    except ApiInstancesHandlerException as ae:
        print(ae)
        del(apis)
        exit(1)

    ### Init backup process ###
    try:
        # Enter NextCloud Maintenance Mode
        enter_maintenance_mode_nextcloud(namespace=lhbe.namespace,app_name=lhbe.app_name)

        # Create MariaDB backup
        create_mysqldump_backup(namespace=lhbe.namespace,db_name=db_name,password=lhbe.db_root_password)

        # Create volumes backup suffix 
        backup_name_suffix=datetime.now().strftime("%d-%m-%Y__%H-%M-%S")

    ### Backup volumes (Nextcloud volume, actual MariaDB volume, backup MariaDB volume) ###

        # Create longhorn nextcloud snapshots and backup
        create_volume_backup_or_snapshot(backup_type=lhbe.backup_type, backup_name_suffix=backup_name_suffix, volume_name=lhbe.app_volume_name, namespace=lhbe.namespace)

        # Create longhorn MariaDB actual volume snapshots and backup
        create_mariadb_volume_backup_or_snapshot()

        # Create longhorn MariaDB backup volume snapshots and backup
        create_volume_backup_or_snapshot(backup_type=lhbe.backup_type, backup_name_suffix=backup_name_suffix, volume_name=lhbe.db_backup_volume_name, namespace=lhbe.namespace)

    except BaseException:
        # Exit NextCloud Maintenance Mode
        exit_maintenance_mode_nextcloud(namespace=lhbe.namespace,app_name=lhbe.app_name)
        exit(1)

    # Exit NextCloud Maintenance Mode
    exit_maintenance_mode_nextcloud(namespace=lhbe.namespace,app_name=lhbe.app_name)

    # Clean old backup/snapshot
    clean_old_backup_snapshot()

    return 0


if __name__ == '__main__':
    main()
    exit(0)

# TODO Add try-except blocks for each critical third-party interaction to nicely release the resources