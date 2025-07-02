import requests
import datetime
import boto3
from pathlib import Path


class APIExtractor:
    VERSION = "v5"
    URL = f"https://api.spacexdata.com/{VERSION}/launches"
    
    def __init__(self, output_path: str = None):
        if output_path is None:
            now = datetime.datetime.now()
            output_path = f"../data/raw/{now.strftime('%Y/%m/%d')}/{now.strftime('%Y%m%d-%H%M%S')}"
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        self.output_path = output_path
        
    def run(self) -> None:
        response = requests.get(self.URL)
        response.raise_for_status()
        if self.output_path.startswith("s3://"):
            bucket_name = self.output_path.split("/")[2]
            key = "".join(self.output_path.split("/")[2:])
            with boto3.client("s3") as client:
                client.put_object(Body=response.text,
                                  Bucket=bucket_name,
                                  Key=key)
        else:
            with open(self.output_path, "w") as f:
                f.write(response.text)
    

if __name__ == "__main__":
    api_extractor = APIExtractor()
    api_extractor.run()
