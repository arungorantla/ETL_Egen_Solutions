from airflow.hooks.postgres_hook import PostgresHook

class DbComponent:
    def __init__(self):
        pass

    def createTable(self, table_name):
        '''
        This function defines schema of county tables and creates new table if not exists
        :param table_name: Name of the county
        :return: None
        '''

        pg_hook = PostgresHook(postgre_conn_id = "postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        query = '''CREATE TABLE IF NOT EXISTS '''+table_name+''' (
                    test_date TIMESTAMP NOT NULL, 
                    new_positives integer NOT NULL, 
                    cumulative_number_of_positives integer NOT NULL,
                    total_number_of_test_performed integer NOT NULL, 
                    cumulative_number_of_test_performed integer NOT NULL,
                    load_date TIMESTAMP NOT NULL
                    );'''
        k = cursor.execute(query)
        conn.commit()
        conn.close()

    def createNYAndUSTable(self, table_name):
        '''
        This function defines schema of Ny and US hospital data tables and creates new table if not exists
        :param table_name: Us or Ny
        :return: None
        '''
        '''(test_date timestamp, new_positives real,
                cumulative_number_of_positives real, total_number_of_test_performed real,
                cumulative_number_of_test_performed real, load_date timestamp)'''

        pg_hook = PostgresHook(postgre_conn_id = "postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        query = '''CREATE TABLE IF NOT EXISTS '''+table_name+''' (
                    id TIMESTAMP PRIMARY KEY, 
                    totalTestResults integer,
                    totalTestResultsIncrease integer,
                    positive integer,
                    positiveIncrease integer,
                    hospitalizedCurrently integer, 
                    inIcuCurrently integer, 
                    onVentilatorCurrently integer, 
                    death integer
                    );'''
        k = cursor.execute(query)
        conn.commit()
        conn.close()

    def deleteTables(self, county_name):
        '''
        This function deletes tables when required
        :param county_name: Table Name of the county to be deleted
        :return: None
        '''

        table_name = county_name
        pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        query2 = "DROP TABLE " + table_name
        k = cursor.execute(query2)
        conn.commit()
        conn.close()

    def createStateTable(self, table_name):
        '''
        This function defines schema of Ny state county data and creates new table if not exists
        :param table_name: Name of the Table
        :return: None
        '''

        pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        query = '''CREATE TABLE IF NOT EXISTS ''' + table_name + ''' (
                            id VARCHAR ( 50 ) PRIMARY KEY,
                            test_date TIMESTAMP NOT NULL, 
                            new_positives integer NOT NULL, 
                            cumulative_number_of_positives integer NOT NULL,
                            total_number_of_test_performed integer NOT NULL, 
                            cumulative_number_of_test_performed integer NOT NULL,
                            load_date TIMESTAMP NOT NULL,
                            state VARCHAR (5) NOT NULL
                            );'''
        k = cursor.execute(query)
        conn.commit()
        conn.close()

    def insertData(self, county_name, **kwargs):
        '''
        This function insert data into the county tables
        :param county_name: Name of the county
        :param kwargs: list of tuples
        :return:
        '''

        ti = kwargs['ti']
        countyMap = ti.xcom_pull(task_ids='load', key='county_insert_data')
        table_name = county_name
        data = countyMap[table_name]
        pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany('INSERT INTO '+ table_name+ ' VALUES(%s, %s, %s, %s, %s, %s)', data)
        conn.commit()
        conn.close()

    def insertNYAndUSData(self, table_name, current_data):
        '''
        This functions insert daily Ny and Us hospital data into the existing tables
        :param table_name: Ny or US
        :param current_data: Data is a list of tuples
        :return:
        '''

        pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany('INSERT INTO '+ table_name+ ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)', current_data)
            conn.commit()
        except:
            pass
        conn.close()

    def getLastDate(self, table_name, col_name="test_date"):
        '''
        This functions gets the latest data form the given table
        :param table_name: Name of table
        :param col_name: test_date
        :return: Latest date in the given table
        '''
        pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM " + table_name + " ORDER BY "+col_name+" DESC LIMIT 1")
        rows = cursor.fetchall()
        for row in rows:
            return row[0]

    def updateData(self, table_name, data):
        '''
        This function updates county table data when required
        :param table_name: county name
        :param data: Data is a list of tuples
        :return: None
        '''
        pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO  " + table_name +
                  " (id, test_date, new_positives, cumulative_number_of_positives, "
                  "total_number_of_test_performed, cumulative_number_of_test_performed, "
                  "load_date, state) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) "
                  "DO UPDATE SET test_date = excluded.test_date, new_positives = excluded.new_positives,"
                  " cumulative_number_of_positives = excluded.cumulative_number_of_positives,"
                  "total_number_of_test_performed = excluded.total_number_of_test_performed,"
                  " cumulative_number_of_test_performed = excluded.cumulative_number_of_test_performed,"
                  "load_date = excluded.load_date, state = excluded.state; ", data[0])
        c = conn.commit()
        print(c)
        conn.close()