from kubencbackup.kubencbackup import KubeNCBackup

if __name__ == '__main__':
    try:
        exit(KubeNCBackup().main())
    except:
        pass
