import boto3
import json

s3_client = boto3.client('s3')

if __name__ == "__main__":
    try: 
        with open('delete_file.json') as json_data:
            items = json.load(json_data)
            for bucket_name in items["s3"]:
                print(f"Emptying Content From: {bucket_name}")
                response = s3_client.list_objects_v2(Bucket=bucket_name)
                if 'Contents' in response:
                    for item in response['Contents']:
                        print('deleting file', item['Key'])
                        s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
                        while response['KeyCount'] == 1000:
                            response = s3_client.list_objects_v2(
                            Bucket=bucket_name,
                            StartAfter=response['Contents'][0]['Key'],
                            )
                            for item in response['Contents']:
                                print('deleting file', item['Key'])
                                s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
                print(f"Bucket: {bucket_name} is Empty")
            
            json_data.close()

    except Exception as e:
        print(f"Error: {e}")


