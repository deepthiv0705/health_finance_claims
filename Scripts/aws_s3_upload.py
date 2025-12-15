import boto3

s3 = boto3.client(
    "s3",
    ##aws_access_key_id="xxx",
    ##aws_secret_access_key="xxx",
    region_name="ap-southeast-2"
)

s3.upload_file("/mnt/c/Users/DEEPTHI/health_finance_claims/Data/csv/claims.csv", "projinsuranceclaim", "raw/claims/claims.csv")
s3.upload_file("/mnt/c/Users/DEEPTHI/health_finance_claims/Data/csv/members.csv", "projinsuranceclaim", "raw/members/members.csv")
s3.upload_file("/mnt/c/Users/DEEPTHI/health_finance_claims/Data/csv/providers.csv", "projinsuranceclaim", "raw/providers/providers.csv")
