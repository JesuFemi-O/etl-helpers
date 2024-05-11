import requests
import re

TAXI_TYPE = "yellow"
START_DATE = "2022-01"

class TaxiAPI:
    def _init_(self, base_url: str) -> None:
        self._base_url = base_url if base_url[-1] == "/" else base_url + "/"
    
    def fetch(self, taxi_type=TAXI_TYPE, date_=START_DATE, csv_format=False, endpoint=None):
        
        date_pattern = "\d{4}-\d{2}"
        _taxi_type = taxi_type.lower()
        if taxi_type not in ('yellow', 'green'):
            raise ValueError(f"{taxi_type} is not a valid taxi_type, expected green or yellow")
        
        if not re.match(date_pattern, date_):
            raise ValueError(f"Invalid date format, expected date in string format YYYY-MM")

        if not endpoint:
            path = f"{_taxi_type}_tripdata{date_}.parquet"
            self.run_date = date_

            target_url = self._base_url + path
        else:
            target_url = self._base_url + endpoint if endpoint[0] != "/" else endpoint[1:]

        try:
            print("fetching data...")
            response = requests.get(target_url)
            print("successfully fetched data")
        except Exception as e:
            raise e

        if response.status_code != 200:
            raise ValueError(f"Expcted status code 200 got {response.status_code} for url {target_url}")

        print("returning context to dagster")
        return response.content