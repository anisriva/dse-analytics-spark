import subprocess
from sys import argv
from shutil import make_archive
from genericpath import isfile, isdir
from os import makedirs, listdir
from socket import gethostname
from os.path import isfile, join, abspath, expanduser

class CommandBuilder:
    def __init__(self, keyspace, count):
        self.hostname = gethostname()
        if not argv[0]=='':
            self.home_dir = abspath(argv[0]).split(argv[0])[0]
        else:
            self.home_dir = expanduser("~")
        self.keyspace = keyspace
        self.count = count

    def get_table_list_command(self):
        command = ['cqlsh', str(self.hostname), 
                    '-e', '''select table_name 
                                from system_schema.tables 
                                where keyspace_name='{}';'''\
                                .format(self.keyspace)
                                ]
                                
        print("Generating table list command : {}".format(command))
        return command
    
    def get_dsbulk_load_command(self, data_load_extract_path, dsbulk_path):
        if not isdir(data_load_extract_path):
            raise Exception("Data extract path not provided")
        elif not isfile(join(dsbulk_path, "dsbulk")):
            raise Exception("dsbulk not found at {} please install".format(dsbulk_path))

        tables = listdir(data_load_extract_path)
        for table in tables:
            yield [join(dsbulk_path, "dsbulk"),
                                "load",
                                "-url", join(data_load_extract_path,table),
                                "-k", self.keyspace,
                                "-t", table,
                                "-header", "true"]

    def get_dsbulk_unload_command(self, dsbulk_path, tables=None):
        # dsbulk_command = "{executable} unload -h {host} -url {target_dir} -query \'{query}\'".split()
        if not tables:
            tables = CommandRunner(self.get_table_list_command()).get_output().split()[2:-2]
        if not isfile(join(dsbulk_path, "dsbulk")):
            raise Exception("dsbulk not found at {} please install".format(dsbulk_path))
        for table in tables:
            dsbulk_command = [join(dsbulk_path, "dsbulk"),"unload", "-h", self.hostname, "-url",join(self.home_dir,"data_extract", table), "-query","select * from \"{}\".\"{}\" limit {}".format(self.keyspace, table, self.count)]
            print("Generating dsbulk command : {}".format(dsbulk_command))
            makedirs(join(self.home_dir,"data_export", table))
            yield dsbulk_command 

class CommandRunner:
    def __init__(self, command):
        self.command = command
        self.output = None
        self.error = None
        self.execute()
    
    def execute(self):
        print("Running command : {}".format(self.command))
        process = subprocess.Popen(self.command, stdout=subprocess.PIPE) 
        self.output, self.error = process.communicate()
    
    def get_error(self):
        return self.error
    
    def get_output(self):
        return self.output



if __name__ == '__main__':
    keyspace = argv[1]
    # data_unload_extract_path = argv[2]
    count = 100
    data_load_extract_path = '/var/lib/cassandra/data_export'
    dsebulk_path = "/opt/dse/dsbulk-1.8.0/bin"
    # dsebulk_path = "/home/animesh.srivastava/dsebulk_install/dsbulk-1.8.0/bin"
    context = CommandBuilder(keyspace, count)
    # all_tables = CommandRunner(context.get_table_list_command()).get_output().split()[2:-2]
    # get_dsbulk_unload_cmd = context.get_dsbulk_unload_command(data_unload_extract_path)
    get_dsbulk_load_cmd = context.get_dsbulk_load_command(data_load_extract_path, dsebulk_path)
    # for cmd in get_dsbulk_unload_cmd:
        # CommandRunner(cmd)
    for cmd in get_dsbulk_load_cmd:
        # print(cmd)
        # print(cmd)
        print(''.join(i+" " for i in cmd))
        # CommandRunner(cmd)
