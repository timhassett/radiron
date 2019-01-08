# s3-bucket-sizes.py
#
# hacked together python script for extracting the size of S3 buckets
# GetBucketSizes takes 2 parameters:
#  - region: the aws region, default is ap-southeast-2
#  - profile: a named profile stored in ~/.aws/credentials

import boto3
import datetime

# borrowed function from: https://stackoverflow.com/questions/12523586/python-format-size-application-converting-b-to-kb-mb-gb-tb/37423778
def humanbytes(B):
   'Return the given bytes as a human friendly KB, MB, GB, or TB string'
   B = float(B)
   KB = float(1024)
   MB = float(KB ** 2) # 1,048,576
   GB = float(KB ** 3) # 1,073,741,824
   TB = float(KB ** 4) # 1,099,511,627,776

   if B < KB:
      return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
   elif KB <= B < MB:
      return '{0:.2f} KB'.format(B/KB)
   elif MB <= B < GB:
      return '{0:.2f} MB'.format(B/MB)
   elif GB <= B < TB:
      return '{0:.2f} GB'.format(B/GB)
   elif TB <= B:
      return '{0:.2f} TB'.format(B/TB)



# function based on post from: https://www.slsmk.com/getting-the-size-of-an-s3-bucket-using-boto3-for-aws/
def GetBucketSizes(region='ap-southeast-2', profile=None):
    now = datetime.datetime.now()
    if profile == None:
        cw = boto3.client('cloudwatch', region_name = region)
        s3client = boto3.client('s3', region_name = region)
    else:
        session = boto3.session.Session(profile_name=profile)
        cw = session.client('cloudwatch', region_name = region)
        s3client = session.client('s3', region_name = region)



    # Get a list of all buckets
    allbuckets = s3client.list_buckets()


    buckets = []


    # Iterate through each bucket
    for bucket in allbuckets['Buckets']:
        # For each bucket item, look up the cooresponding metrics from CloudWatch
        response = cw.get_metric_statistics(Namespace='AWS/S3',
                                        MetricName='BucketSizeBytes',
                                        Dimensions=[
                                            {'Name': 'BucketName', 'Value': bucket['Name']},
                                            {'Name': 'StorageType', 'Value': 'StandardStorage'}
                                        ],
                                        Statistics=['Average'],
                                        Period=3600,
                                        StartTime=(now-datetime.timedelta(days=2)).isoformat(),
                                        EndTime=now.isoformat()
                                        )
        # The cloudwatch metrics will have the single datapoint, so we just report on it. 
        for item in response["Datapoints"]:
            size = int(item["Average"])
            buckets.append([bucket["Name"], item["Average"]])

    sorted_by_size = sorted(buckets, key=lambda tup: tup[1], reverse=True)

    print('==========================================================================================')
    print('Profile/Account: ' + profile + '\t\tRegion: ' + region)
    print('Bucket'.ljust(60) + 'Size in Bytes'.rjust(25))
    print('------------------------------------------------------------------------------------------')
    for bucket in sorted_by_size:
        print(bucket[0].ljust(60) + str(humanbytes(bucket[1])).rjust(25))



# usage:
# GetBucketSizes(profile='<profile name'>)
# GetBucketSizes(region='ap-southeast-1', profile='<profile name>')
