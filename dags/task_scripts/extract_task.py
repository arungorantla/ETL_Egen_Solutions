import json, requests, string

class Extract:
    def __init__(self):
        pass

    def extract(self,**kwargs):

        ti = kwargs['ti']
        data_string_county = self.getDataFromAPI()
        data_string_ny = self.getDailyNyDataFromAPI()
        data_string_us = self.getDailyUSDataFromAPI()
        ti.xcom_push('covid_test_data', data_string_county)
        ti.xcom_push('ny_hospital_data', data_string_ny)
        ti.xcom_push('us_hospital_data', data_string_us)

    def getDataFromAPI(self):
        '''This function gets daily county testing data for the state of New York'''

        response = requests.get("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD")
        api_data = response.json()["data"]
        result_data = []
        for row in api_data:
            county_name = row[9]
            data = [county_name, row[8]]
            data += row[10:]
            result_data.append(data)
        return result_data

    def getCountyNamesFromAPI(self):
        '''This functions returns the list of counties in the state of New York'''

        data = self.getDataFromAPI()
        countnames = set()
        for row in data:
            county_name = row[0]
            exclude = set(string.punctuation)
            county_name = ''.join("_" if i in exclude else i for i in county_name)
            county_name = county_name.replace(" ", "_")
            countnames.add(county_name)
        return list(countnames)

    def getDailyNyDataFromAPI(self):
        '''This function gets hospital and covid test data for the state of New York'''

        response = requests.get("https://api.covidtracking.com/v1/states/Ny/daily.json")
        api_data = response.json()
        return self.__extract_hospital_data_lib(api_data)

    def getDailyUSDataFromAPI(self):
        '''This function gets hospital and covid test data for the all the states in US combined'''

        response = requests.get("https://api.covidtracking.com/v1/us/daily.json")
        api_data = response.json()
        return self.__extract_hospital_data_lib(api_data)


    def __extract_hospital_data_lib(self,api_data):
        '''This function extracts required columns from the api response'''

        keys_to_extract = ["date", "totalTestResults", "totalTestResultsIncrease", "positive", "positiveIncrease",
                           "hospitalizedCurrently", "inIcuCurrently",
                           "onVentilatorCurrently", "death"]
        state_data = []
        for data in api_data:
            temp = []
            for key in keys_to_extract:
                temp.append(data[key])
            state_data.append(temp)
        return state_data