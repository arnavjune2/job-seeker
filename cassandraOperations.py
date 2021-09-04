from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import tuple_factory
import pandas as pd


class CassandraManagement:

    def __init__(self, clientID, clientSec, keyspace, secure_connect):

        try:
            self.clientID = clientID
            self.clientSec = clientSec
            self.keyspace = keyspace
            self.secure_connect = secure_connect
            cloud_config = {
                'secure_connect_bundle': f'{secure_connect}'
            }
            auth_provider = PlainTextAuthProvider(clientID, clientSec)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute(f"use {keyspace}").one()
            if row:
                print(row[0])
            else:
                print("An error occurred.")
        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))

    def getCassandraClientObject(self):
        """
        This function creates the client object for connection purpose
        """
        try:
            cloud_config = {
                'secure_connect_bundle': 'secure-connect-test1.zip'
            }
            auth_provider = PlainTextAuthProvider(self.clientID, self.clientSec)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect('job_seeker')
            return session
        except Exception as e:
            raise Exception("(getCassandraClientObject): Something went wrong on creation of client object\n" + str(e))

    def closeCassandraSession(self, cass_client):
        """
        This function closes the connection of client
        :return:
        """
        try:
            cass_client.shutdown()
        except Exception as e:
            raise Exception(f"Something went wrong on closing session\n" + str(e))

    def isKeyspacePresent(self, keyspace):
        """
        This function checks if the database is present or not.
        :param keyspace:
        :return:
        """
        try:
            cass_client = self.getCassandraClientObject()
            rows = cass_client.execute("SELECT * from system_schema.keyspaces")
            keyspaces = [i[0] for i in list(rows)]
            if keyspace in keyspaces:
                self.closeCassandraSession(cass_client)
                return True
            else:
                cass_client.shutdown()
                return False
        except Exception as e:
            raise Exception("(isKeyspacePresent): Failed on checking if the database is present or not \n" + str(e))

    def getTable(self, table_name):
        """
        This returns table.
        :return:
        """

        try:
            query = f"SELECT * FROM {table_name}"
            cass_client = self.getCassandraClientObject()
            cass_client.row_factory = tuple_factory()
            df = cass_client.execute(query)
            table = pd.DataFrame(list(df))
            return table
        except Exception as e:
            raise Exception(f"(getTable): Failed to get {table_name}")

    def dropDatabase(self, table_name):
        """
        :param table_name:
        :return: status
        """
        try:
            session = self.getCassandraClientObject()
            session = session.execute(f"drop {table_name}")
            return "table dropped"
        except Exception as e:
            raise Exception("table didn't dropped successfully")

    def insertRecordinMaster(self, table_name, record: list):
        """

        This inserts a record.
        :param table_name:
        :param record:
        give the record in list
        :return:
        """

        try:
            # getting all the columns names
            session = self.getCassandraClientObject()
            id = record[0]
            company = record[1]
            employer_mail = record[2]
            job_desc = record[3]
            skills = record[4]
            session.execute(f"INSERT INTO master ( id , company , employer_mail , job_desc , skills )  "
                            f"VALUES ( {id} , {company} , {employer_mail} , {job_desc} , {skills} ) ;")

            self.closeCassandraSession(session)

            return f"rows inserted in Master"
        except Exception as e:
            raise Exception(f"(insertRecordinMaster): Something went wrong on inserting record in Master\n" + str(e))

    def insertRecordinlogin(self, table_name, record):
        """

        This inserts a record.

        :param table_name:
        :param record:
        insert in following sequence
        email ,
        first_name ,
        last_name ,
        password ,
        phone_no ,
        status
        :return:
        """

        try:
            session = self.getCassandraClientObject()


            email = record[1]
            job_title = record[2]
            company = record[3]
            job_desc = record[4]
            skills = record[5]

            session.execute(
                f"INSERT INTO {table_name} (id , email ,[first_name , last_name , password , phone_no , status"
                f"VALUES (uuid() , {job_title} , {company} , {job_desc},{skills})")
            self.closeCassandraSession()

            return f"rows inserted in login "
        except Exception as e:
            raise Exception(f"(insertRecordinlogin): Something went wrong on inserting record in login\n" + str(e))

    def findfirstRecord(self, table_name, query=None):
        """
        this fuction will only return the first record in the table and
        in the end of query please write LIMIT 1
        """

        try:
            session = self.getCassandraClientObject()
            session.row_factory = tuple_factory()
            row = session.execute(query)
            return row[0]
        except Exception as e:
            raise Exception(f"(findfirstRecord): Failed to find first record\n" + str(e))

    def findAllRecords(self, table_name, query):
        """
        :param table_name
        :param query
        it return a <cassandra.cluster.ResultSet > so we have to iterate over it

        """

        try:
            session = self.getCassandraClientObject()
            session.row_factory = tuple_factory
            AllRecords = session.execute(query)

            return AllRecords
        except Exception as e:
            raise Exception(f"(findAllRecords): Failed to find record for the given table\n" + str(e))

    def deleteRecord(self, table_name, id):
        """
        :param table_name
        :param id
        """

        try:
            session = self.getCassandraClientObject()
            session.execute(f"DELETE FROM {table_name} where id={id}")
            return "1 row deleted"
        except Exception as e:
            raise Exception(
                f"(deleteRecord): Failed remove a record.\n" + str(e))
