import json
import requests
import requests
import boto3
import os
data_url = os.environ['DATA_URL']
# "https://services5.arcgis.com/Rvw11bGpzJNE7apK/arcgis/rest/services/VaccinesPublic_gdb/FeatureServer/0/query?f=json&where=1=1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&maxRecordCountFactor=4&outSR=102100&resultOffset=0&resultRecordCount=8000&cacheHint=true&quantizationParameters={%22mode%22:%22view%22,%22originPosition%22:%22upperLeft%22,%22tolerance%22:1.0583354500042335,%22extent%22:{%22xmin%22:-11864749.745014807,%22ymin%22:2986125.9341450464,%22xmax%22:-10435895.933896504,%22ymax%22:4355735.017881977,%22spatialReference%22:{%22wkid%22:102100,%22latestWkid%22:3857}}}"
sns_arn = os.environ['SNS_ARN']
sns_client = boto3.client('sns')
def lambda_handler(event, context):
    # Pull data and get inventory info
    data = requests.get(data_url).json()["features"]
    data_list = map(lambda item: item["attributes"], data)
    data_list_in_stock = list(filter(lambda x: x["Total_Available"] > 0, data_list)) 
    data_list_in_stock.sort(key=lambda x:x["Total_Available"])
    if len(data_list_in_stock) > 0:
        # send SNS, note we are only sending top 5
        response = sns_client.publish(
            TargetArn=sns_arn,
            Message=json.dumps({"default": json.dumps(data_list_in_stock[0:5])}),
            MessageStructure='json'
        )

    return {
        'statusCode': 200,
        'body': json.dumps(data_list_in_stock)
    }
