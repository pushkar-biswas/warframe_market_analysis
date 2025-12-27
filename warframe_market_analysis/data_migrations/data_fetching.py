import time
import pandas as pd
import requests
import json


# global variables
expected_key_values = [
	"id",                   # Is the unique identifier of the order.
	"type",                 # Specifies whether the order is a 'buy' or 'sell'.
	"platinum",             # Is the total platinum currency involved in the order.
	"quantity",             # Represents the number of items included in the order.
	"rank",                 # (optional) specifies the rank or level of the item in the order.
	"charges",              # (optional) specifies number of charges left (used in requiem mods).
	"subtype",              # (optional) defines the specific subtype or category of the item.
	"amberStars",           # (optional) denotes the count of amber stars in a sculpture order.
	"cyanStars",            # (optional) denotes the count of cyan stars in a sculpture order.
	"createdAt",            # Records the creation time of the order.
	"itemId",               # Is the unique identifier of the item involved in the order.
    "group",                # User-defined group to which the order belongs
]


def api_call():

    url = "https://api.warframe.market/v2/orders/recent"

    payload = {}
    headers = {
        'Language': 'en'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    raw_data = json.loads(response.text)
    return raw_data

def data_to_append(key_values, new_order):
    order_info = {}

    for key_val in key_values:
        if key_val in key_values:
            order_info[key_val]: new_order[key_values]


    return order_info


def process_raw_data(raw_data):

    orders = []

    for new_order in raw_data["data"]:

        user_info = new_order["user"]

        order_info = data_to_append(list(new_order.keys()), new_order)

        order_info = {
            "id":         user_info['id'],               # Unique identification of user
            "reputation": user_info['reputation'],       # Number of times other players gave an upvote to the player
            "platform":   user_info['platform'],         # Current platform of the player (PC, XBOX, PS, SWITCH, Mobile)
            "crossplay":  user_info['crosplay'],         # Weather the player accepts
            "locale":     user_info['locale'],
            "status":     user_info['status'],
            "lastSeen":   user_info['lastSeen']
        }

        print(type(order_info))
        orders.append(order_info)


    return pd.DataFrame(orders)


while True:

    raw_data = api_call()
    orders_df = process_raw_data(raw_data)
    print(orders_df)




    time.sleep(60)