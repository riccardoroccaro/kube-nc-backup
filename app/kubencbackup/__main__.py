from datetime import datetime
from time import sleep

from lh_backup_environment_handler import LHBackupEnvironmentException
from lh_backup_environment_handler import LHBackupEnvironment

# Declaring class private const
ENTER_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --on'
EXIT_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --off'


def enter_nextcloud_maintenance_mode(namespace, app_name):
    resp = exec_container_command(namespace=namespace,app_name=app_name, command=ENTER_MAINTENANCE_CMD)
    if resp == "Maintenance mode enabled\n":
        sleep(30)
    else:
        print("Error entering maintenance mode")
        exit_nextcloud_maintenance_mode(namespace=namespace,app_name=app_name)
        exit(1)


def exit_nextcloud_maintenance_mode(namespace, app_name):
    resp = exec_container_command(namespace=namespace,app_name=app_name, command=EXIT_MAINTENANCE_CMD)
    if resp != "Maintenance mode disabled\n":
        print("Error exiting maintenance mode. You have to do it by hands")
        exit(1)


#TODO Add backup file path
def create_mysqldump_backup(namespace,db_name,password):
    command = "mysqldump --add-drop-database --add-drop-table --lock-all-tables --result-file=backup.sql --password="+password+" --all-databases"
    exec_container_command(namespace=namespace,app_name=db_name,command=command)


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
        enter_nextcloud_maintenance_mode(namespace=lhbe.namespace,app_name=lhbe.app_name)

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
        exit_nextcloud_maintenance_mode(namespace=lhbe.namespace,app_name=lhbe.app_name)
        exit(1)

    # Exit NextCloud Maintenance Mode
    exit_nextcloud_maintenance_mode(namespace=lhbe.namespace,app_name=lhbe.app_name)

    # Clean old backup/snapshot
    clean_old_backup_snapshot()

    return 0


if __name__ == '__main__':
    main()
    exit(0)
