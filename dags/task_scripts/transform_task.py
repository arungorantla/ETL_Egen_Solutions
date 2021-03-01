# [START import_module]
from datetime import datetime, timedelta
from collections import defaultdict
import string
# [END import_module]

class Transform:
    def __init__(self):
        pass

    def transform(self, **kwargs):
        ti = kwargs['ti']
        covid_test_data = ti.xcom_pull(task_ids='extract', key='covid_test_data')
        transformed_covid_test_data = self.__transform_covid_test_data(covid_test_data)

        ny_hospital_data = ti.xcom_pull(task_ids='extract', key='ny_hospital_data')
        transformed_ny_hospital_data = self.__transform_ny_and_us_hospital_data(ny_hospital_data)

        us_hospital_data = ti.xcom_pull(task_ids='extract', key='us_hospital_data')
        transformed_us_hospital_data = self.__transform_ny_and_us_hospital_data(us_hospital_data)

        ti.xcom_push('transformed_covid_test_data', transformed_covid_test_data)
        ti.xcom_push('transformed_ny_hospital_data', transformed_ny_hospital_data)
        ti.xcom_push('transformed_us_hospital_data', transformed_us_hospital_data)

    def __transform_covid_test_data(self, api_data):
        '''This function perform data cleaning and append required new column to the covid test data tables'''

        countyData = defaultdict(list)
        for row in api_data:
            county_name = row[0]
            exclude = set(string.punctuation)
            county_name = ''.join("_" if i in exclude else i for i in county_name)
            county_name = county_name.replace(" ", "_")
            data = row[1:]
            data.append('{}'.format(datetime.now()))
            countyData[county_name].append(data)
        return countyData

    def __transform_ny_and_us_hospital_data(self, api_data):
        '''This function perform data cleaning and append required new column to the hospital data tables'''

        result_data = []

        for data in api_data:
            dt = datetime.strptime(str(data[0]), "%Y%m%d")
            dt = '{}'.format(dt)
            temp = [dt]
            temp +=data[1:]
            result_data.append(temp)
        return result_data