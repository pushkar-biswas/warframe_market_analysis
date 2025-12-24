import time
import pandas as pd
import requests
import json

def api_call():

    url = "https://api.warframe.market/v2/orders/recent"

    payload = {}
    headers = {
        'Language': 'en'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    raw_data = json.loads(response.text)
    return raw_data

def process_raw_data(raw_data):

    orders = []
    for new_order in raw_data["data"]:

        user_info = new_order["user"]

        order_info = {
            "order_id" : new_order["id"],
            "order_type": new_order["type"],
            "plt_amt": new_order["platinum"],
            "order_qty": new_order["quantity"],
            "item_rank": new_order["rank"],
            "order_created": new_order["createdAt"],
            "item_id": new_order["itemId"]
        }
        orders.append(order_info)


    return pd.DataFrame(orders)


while True:

    raw_data = api_call()
    orders_df = process_raw_data(raw_data)
    print(orders_df)




    time.sleep(60)