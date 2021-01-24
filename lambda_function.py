import json
import requests
import pandas as pd
import requests
data_url = "https://services5.arcgis.com/Rvw11bGpzJNE7apK/arcgis/rest/services/VaccinesPublic_gdb/FeatureServer/0/query?f=json&where=1=1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&maxRecordCountFactor=4&outSR=102100&resultOffset=0&resultRecordCount=8000&cacheHint=true&quantizationParameters={%22mode%22:%22view%22,%22originPosition%22:%22upperLeft%22,%22tolerance%22:1.0583354500042335,%22extent%22:{%22xmin%22:-11864749.745014807,%22ymin%22:2986125.9341450464,%22xmax%22:-10435895.933896504,%22ymax%22:4355735.017881977,%22spatialReference%22:{%22wkid%22:102100,%22latestWkid%22:3857}}}"

def lambda_handler(event, context):
    # Pull data and get inventory info
    data = requests.get(data_url).json()["features"]
    data_list = map(lambda item: item["attributes"], data)
    # load data into dataframe
    df = pd.DataFrame(data_list)
    # fitler by total available
    df_in_stock = df[df["Total_Available"] > 0]
    # sort by inventory
    df_in_stock = df_in_stock.sort_values("Total_Available", ascending=False)
    # take top 5 inventory
    df_in_stock_tops = df_in_stock.head(5)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
