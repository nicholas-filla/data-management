from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
import pandas as pd

class SQLManager:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.server_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}"
        self.current_engine = None

    def __connect(self, connection_string):
        try:
            self.current_engine = create_engine(connection_string)
            self.current_engine.connect()

            # Extract the database name from the connection string
            split_string = connection_string.split('@')[1]
            if '/' in split_string:
                database_name = split_string.split('/')[1]
                print(f"Successfully connected to the database '{database_name}'")
            else:
                print("Successfully connected to the server")
            
            return self.current_engine
        except SQLAlchemyError as e:
            print(f"The error '{e}' occurred")
            self.current_engine = None
            return None

    def __disconnect(self):
        if self.current_engine:
            self.current_engine.dispose()
            print("Connection closed")
            self.current_engine = None

    def check_database_exists(self, database):
        engine = self.__connect(self.server_string)
        if engine:
            with engine.connect() as conn:
                try:
                    result = conn.execute(text(f"SHOW DATABASES LIKE '{database}'"))
                    exists = result.fetchone() is not None
                    if exists:
                        print(f"Database '{database}' exists")
                    else:
                        print(f"Database '{database}' does not exist")
                    return exists
                except SQLAlchemyError as e:
                    print(f"Error occurred while checking database existence: {e}")
                finally:
                    self.__disconnect()
        return False

    def show_databases(self):
        engine = self.__connect(self.server_string)
        if engine:
            with engine.connect() as conn:
                try:
                    result = conn.execute(text("SHOW DATABASES"))
                    databases = result.fetchall()
                    if databases:
                        print("Databases on the server:")
                        for database in databases:
                            print(database[0])
                    else:
                        print("No databases found on the server.")
                except SQLAlchemyError as e:
                    print(f"Error occurred while retrieving databases: {e}")
                finally:
                    self.__disconnect()

    def check_table_exists(self, database, table_name):
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}"
        engine = self.__connect(connection_string)
        if engine:
            with engine.connect() as conn:
                try:
                    result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                    exists = result.fetchone() is not None
                    if exists:
                        print(f"Table '{table_name}' exists in database '{database}'")
                    else:
                        print(f"Table '{table_name}' does not exist in database '{database}'")
                    return exists
                except SQLAlchemyError as e:
                    print(f"Error occurred while checking table existence: {e}")
                finally:
                    self.__disconnect()
        return False

    def show_tables(self, database):
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}"
        engine = self.__connect(connection_string)
        if engine:
            with engine.connect() as conn:
                try:
                    result = conn.execute(text("SHOW TABLES"))
                    tables = result.fetchall()
                    if tables:
                        print(f"Tables in database '{database}':")
                        for table in tables:
                            print(table[0])
                    else:
                        print(f"No tables found in database '{database}'.")
                except SQLAlchemyError as e:
                    print(f"Error occurred while retrieving tables: {e}")
                finally:
                    self.__disconnect()

    def create_database(self, database):
        if not self.check_database_exists(database):
            engine = self.__connect(self.server_string)
            if engine:
                with engine.connect() as conn:
                    try:
                        conn.execute(text(f"CREATE DATABASE {database}"))
                        print(f"Database '{database}' created successfully")
                    except SQLAlchemyError as e:
                        print(f"Error occurred while creating database: {e}")
                    finally:
                        self.__disconnect()
        else:
            print(f"Database '{database}' already exists")

    def delete_database(self, database):
        if self.check_database_exists(database):
            engine = self.__connect(self.server_string)
            if engine:
                with engine.connect() as conn:
                    try:
                        conn.execute(text(f"DROP DATABASE {database}"))
                        print(f"Database '{database}' deleted successfully")
                    except SQLAlchemyError as e:
                        print(f"Error occurred while deleting database: {e}")
                    finally:
                        self.__disconnect()
        else:
            print(f"Database '{database}' does not exist")

    def delete_table(self, database, table_name=None):
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}"
        engine = self.__connect(connection_string)
        if engine:
            with engine.connect() as conn:
                try:
                    if table_name:
                        if self.check_table_exists(database, table_name):
                            conn.execute(text(f"DROP TABLE {table_name}"))
                            print(f"Table '{table_name}' deleted successfully from database '{database}'")
                        else:
                            print(f"Table '{table_name}' does not exist in database '{database}'")
                    else:
                        tables = conn.execute(text("SHOW TABLES")).fetchall()
                        if tables:
                            for table in tables:
                                conn.execute(text(f"DROP TABLE {table[0]}"))
                            print(f"All tables deleted successfully from database '{database}'")
                        else:
                            print(f"No tables found in database '{database}'")
                except SQLAlchemyError as e:
                    print(f"Error occurred while deleting table(s): {e}")
                finally:
                    self.__disconnect()

    def export_table_to_sql(self, data_frame, database, table_name, if_exists='fail'):
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}"
        engine = self.__connect(connection_string)
        if engine:
            try:
                data_frame.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
                print(f"DataFrame exported to table '{table_name}' in database '{database}' successfully")
            except SQLAlchemyError as e:
                print(f"Error occurred while exporting DataFrame to table: {e}")
            finally:
                self.__disconnect()

    def import_table_from_sql(self, database, table_name):
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}"
        engine = self.__connect(connection_string)
        if engine:
            try:
                data_frame = pd.read_sql_table(table_name, con=engine)
                print(f"Data imported from table '{table_name}' in database '{database}' successfully")
                return data_frame
            except SQLAlchemyError as e:
                print(f"Error occurred while importing data from table: {e}")
                return None
            finally:
                self.__disconnect()
