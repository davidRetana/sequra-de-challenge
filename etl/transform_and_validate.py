import json
import pandas as pd
from pathlib import Path
from jsonschema import validate


class TransformAndValidate:
    
    def __init__(self, input_path: str, api_version: str):
        self.input_path = input_path
        self.api_version = api_version
        
    def run(self) -> None:
        df = pd.read_json(input_path)
        schema = self.read_schema()
        status = self.validate_data(df, schema)
        if status["is_valid"]:
            output_path = input_path.replace("raw", "validated").replace("json", "parquet")
            self.create_directory(output_path)
            # df = self.transform_df(df)
            df.to_parquet(output_path)
        else:
            output_path = input_path.replace("raw", "failed")
            self.create_directory(output_path)
            df.to_json(output_path)
            output_path = output_path.replace("json", "log")
            self.create_directory(output_path)
            self.write_log(status["error"], output_path)

    def read_schema(self) -> dict:
        with open(f"../data/schemas/{self.api_version}/{self.api_version}_schema.json", "r") as f:
            schema = json.load(f)
        return schema

    def validate_data(self, df: pd.DataFrame, schema: dict) -> dict:
        output = {
            "is_valid": True,
            "error": None
        }
        try:
            validate(instance=df.to_dict(orient="records"), schema=schema)
            print("JSON is valid")
        except Exception as e:
            print("Schema validation failed")
            output["valid"] = False
            output["error"] = e
        finally:
            return output
    
    def write_log(self, exception: Exception, output_path: str) -> None:
        with open(output_path, "w") as f:
            f.write(str(exception))
    
    def create_directory(self, path: str):
        Path(path.rsplit("/", 1)[0]).mkdir(parents=True, exist_ok=True)
    
    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        fairings_df = pd.json_normalize(df["fairings"].to_list(), )
        cores_df = df["cores"].dropna().explode()
        cores_normalized = pd.json_normalize(cores_df.tolist())
        # normalize and extract all the fields neccesary here, not doing so for time reason
        df_combined = pd.concat([df.drop(columns=["fairings", "cores"]), fairings_df, cores_normalized], axis=1)
        return df_combined
    

if __name__ == "__main__":
    input_path = "../data/raw/2025/06/27/20250627-151738.json"
    api_version = "v5"
    process = TransformAndValidate(input_path, api_version)
    process.run()
