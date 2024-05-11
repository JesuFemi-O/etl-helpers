from google.cloud import storage
from google.oauth2 import service_account
import os

class GoogleCloudStoarage:
    def _init_(self, bucket_name, credentials_path=None) -> None:
        self.token_path = credentials_path
        self.bucket_name = bucket_name
        self.gcs_client = self._start_client()
    
    def _start_client(self):
        
        if self.token_path:
            _credentials = service_account.Credentials.from_service_account_file(self.token_path)
            return storage.Client(credentials=_credentials)
        else:
            # try to pull default credentials
            try:
                # TODO: kindly refactor, this is an overkill!!
                default_credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
                _credentials = service_account.Credentials.from_service_account_file(default_credentials_path)
                return storage.Client(credentials=_credentials)
            except KeyError:
                raise KeyError('explicitly provide credentials_path or set env variable GOOGLE_APPLICATION_CREDENTIALS')
            except Exception as e:
                raise(e)
    
            
    def write_to_gcs(self, file_name, content):
        bucket = self.gcs_client.bucket(self.bucket_name)
        
        # Create a blob object from the content
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)