#!/usr/bin/env python3

# Execute like so:
# python3 time-awwter.py <keyspace> <table> <host> --user dba --ssl-certificate <path_to_file> --ssl-key <path_to_file> --pr-key-list <path_to_file>
# This script will iterate through a list of primary keys and print tracing messages for each request. The list specified with --pr-key-list is a new line separated list of primary keys.

# A lot of things were plagiarized from cassandra-trireme project https://github.com/fxlv/cassandra-trireme.git


# Pyton libs
from ssl import SSLContext, PROTOCOL_TLSv1, PROTOCOL_TLSv1_2
import argparse
import sys

# Cassandra related libs
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider

# Settings
import settings

# A few datastructures that we're gonna use:
class CassandraSettings:

    def __init__(self):
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.ssl_cert = None
        self.ssl_key = None
        self.ssl_version = None

class AppSettings:

    def __init__(self):
        self.pr_keys = None
        self.keyspace = None
        self.table = None

class PrimaryKeys:
    def __init__(self):
        self.pr_keys_list = []

    def __getattr__(self, name):
        return getattr(self.pr_keys_list, name)
    
    @classmethod
    def pr_key_list_creator(cls, filename):
        self = cls()
        with open(filename, 'r') as f:
            for row in f:
                self.pr_keys_list.append(row.strip())
            return self

    def __len__(self):
        return len(self.pr_keys_list)

    def __iter__(self):
        return self.pr_keys_list.__iter__()


def parse_user_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.description = "Row reader for tracing and debugging"
    parser.add_argument("host", type=str, help="Cassandra host")
    parser.add_argument("keyspace", type=str, help="Keyspace to use")
    parser.add_argument("table", type=str, help="Table to use")
    parser.add_argument("--port",
                        type=int,
                        default=9042,
                        help="Cassandra port (9042 by default)")
    parser.add_argument("--user",
                        type=str,
                        default="cassandra",
                        help="Cassandra username")
    parser.add_argument("--password",
                        type=str,
                        help="Path to a file with a 'db_password = passwd'")    
    parser.add_argument("--ssl-certificate",
                        dest="ssl_cert",
                        type=str,
                        help="SSL certificate to use")
    parser.add_argument("--ssl-key",
                        dest="ssl_key",
                        type=str,
                        help="Key for the SSL certificate")
    parser.add_argument("--ssl-version",
                        type=str,
                        default="PROTOCOL_TLSv1_2",
                        dest="ssl_version",
                        help="Key for the SSL certificate")    
    parser.add_argument("--pr-key-list",
                        dest="pr_keys",
                        type=str,
                        help="A file with a list of primary keys separated by new line")
    args = parser.parse_args()
    return args


def get_cassandra_session(host,
                          port,
                          user,
                          password,
                          ssl_cert,
                          ssl_key,
                          ssl_version):

    auth_provider = PlainTextAuthProvider(username=user, password=password)

    ssl_options = {
        'certfile': ssl_cert,
        'keyfile': ssl_key,
        'ssl_version': PROTOCOL_TLSv1_2
        }

    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.LOCAL_ONE
        )

    cluster = Cluster([host], port=port, ssl_options=ssl_options, auth_provider=auth_provider, execution_profiles={EXEC_PROFILE_DEFAULT: profile})

    session = cluster.connect()
    return session


def execute_select(keyspace,
                    table,
                    primary_keys,
                    session):

    sql_template="SELECT * FROM {}.{} WHERE userhash=%s".format(keyspace, table)
    
    result_list = []

    for u in primary_keys:
        result_list.append(session.execute_async(sql_template, [u], trace=True))
    
    for item in result_list:
        rows = item.result()
        trace = item.get_query_trace()
        for e in trace.events:
            print(e.source_elapsed, e.description)
    

if __name__ == "__main__":

    # Instantiate the cassandra settings class
    cas_settings = CassandraSettings()    
    # Set attributes for the instance
    args = parse_user_args()
    cas_settings.host = args.host
    cas_settings.port = args.port
    cas_settings.user = args.user

    if hasattr(settings, "db_password"):
        cas_settings.password = settings.db_password
    else:
        cas_settings.password = args.password

    cas_settings.ssl_cert = args.ssl_cert
    cas_settings.ssl_key = args.ssl_key
    cas_settings.ssl_version = args.ssl_version

    # Instantiate appsettings class
    app_settings = AppSettings()
    # Set attributes for the insantce
    app_settings.pr_keys = args.pr_keys
    app_settings.keyspace = args.keyspace
    app_settings.table = args.table

    # Instantiate a list of primary keys
    primary_keys = PrimaryKeys.pr_key_list_creator(app_settings.pr_keys)

    # Login to the db and get the session object
    runtime_session = get_cassandra_session(cas_settings.host, cas_settings.port, cas_settings.user,  cas_settings.password, cas_settings.ssl_cert, cas_settings.ssl_key, cas_settings.ssl_version)

    # Execut the tracing select query
    execute_select(app_settings.keyspace, app_settings.table, primary_keys, runtime_session)
