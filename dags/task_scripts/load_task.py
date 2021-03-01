from .db_component import DbComponent
from collections import defaultdict

class Load:
    def __init__(self):
        self.sql_comp = DbComponent()

    def load(self, **kwargs):
        ti = kwargs['ti']
        covid_test_data = ti.xcom_pull(task_ids='transform', key='transformed_covid_test_data')
        county_insert_data = self.__load_covid_test_data(covid_test_data)
        ti.xcom_push('county_insert_data', county_insert_data)

        ny_hospital_data = ti.xcom_pull(task_ids='transform', key='transformed_ny_hospital_data')
        self.__load_ny_hospital_data(ny_hospital_data)

        us_hospital_data = ti.xcom_pull(task_ids='transform', key='transformed_us_hospital_data')
        self.__load_us_hospital_data(us_hospital_data)


    def __load_covid_test_data(self, county_data):
        '''This function loads county data into all the county tables'''

        self.sql_comp.createStateTable("new_york_state")
        data_to_insert_map = defaultdict(list)

        for county_name, data in county_data.items():
            print(county_name)
            self.sql_comp.createTable(county_name)
            last_date = self.sql_comp.getLastDate(county_name)
            data_to_insert = []
            for row in data:
                if not last_date or row[0] > 'T'.join('{}'.format(last_date).split(" ")):
                    data_to_insert.append(row)
            if data_to_insert:
                self.sql_comp.updateData("new_york_state", [[county_name] + data_to_insert[-1] + ["NY"]])
            print(len(data_to_insert))
            data_to_insert_map[county_name] = data_to_insert
        return data_to_insert_map


    def __load_ny_hospital_data(self, data):
        '''This function loads data into ny_hospital_data table'''

        self.sql_comp.createNYAndUSTable("ny_hospital_data")
        ny_last_date = self.sql_comp.getLastDate("ny_hospital_data", "ID")
        print("ny", ny_last_date)
        data_to_insert = []
        for row in data:
            if not ny_last_date or row[0] > 'T'.join('{}'.format(ny_last_date).split(" ")):
                data_to_insert.append(row)
        print("ny",len(data_to_insert))
        self.sql_comp.insertNYAndUSData("ny_hospital_data", data_to_insert)

    def __load_us_hospital_data(self, data):
        '''This function loads data into us_hospital_data table'''

        self.sql_comp.createNYAndUSTable("us_hospital_data")
        us_last_date = self.sql_comp.getLastDate("us_hospital_data", "ID")
        print("us", us_last_date)
        data_to_insert = []
        for row in data:
            if not us_last_date or row[0] > 'T'.join('{}'.format(us_last_date).split(" ")):
                data_to_insert.append(row)
        print("us", len(data_to_insert))
        self.sql_comp.insertNYAndUSData("us_hospital_data", data)