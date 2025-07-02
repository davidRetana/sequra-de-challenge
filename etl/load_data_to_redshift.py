import redshift_connector


class Loader:
    
    REDSHIFT_HOST = "foo_host"
    REDSHIFT_PORT = 5439
    REDSHIFT_DB = "foo_db"
    REDSHIFT_USER = "foo_user"
    REDSHIFT_PASSWORD = "foo_password"  # just only for this challenge, in production we can retrieve password using
    # AWS secret manager or even better, use a managed identity or similar authentication flow

    def __init__(self, s3_path: str):
        self.s3_path = s3_path
        self.iam_role = "foo"
    
    def get_connection(self):
        return redshift_connector.connect(
            host=self.REDSHIFT_HOST,
            port=self.REDSHIFT_PORT,
            database=self.REDSHIFT_DB,
            user=self.REDSHIFT_USER,
            password=self.REDSHIFT_PASSWORD,
        )
    
    def load(self):
        with self.get_connection() as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            
            cursor.execute(f"""
                    COPY staging_table
                    FROM '{self.s3_path}'
                    IAM_ROLE '{self.iam_role}'
                    FORMAT AS PARQUET;
                """)
            
            cursor.execute("""
                    MERGE INTO final_table AS target
                    USING staging_table AS source
                    ON target.id = source.id
                    WHEN MATCHED THEN
                      UPDATE SET
                        field1 = source.field1,
                        field2 = source.field2,
                        ...
                    WHEN NOT MATCHED THEN
                      INSERT (id, field1, field2)
                      VALUES (source.id, source.field1, source.field2, ...);
                        """)
            
    
if __name__ == "__main__":
    s3_path = "s3://bucket..."
    loader = Loader(s3_path)
