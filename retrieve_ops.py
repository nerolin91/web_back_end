''' Retrieve operations for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def retrieve_by_id_old(table, id, response):
    print "Retrieve by id not yet implemented"

    response.status = 501
    
    return {"errors": [{
        "retrieve by id not yet 1implemented": {"id": id}
        }]}

def retrieve_by_id(table, id, response):
    try:
        item = table.get_item(id=id)
        name = item["name"]
        activities = item["activities"]

        if activities is None:
            activities = []

        response.status = 200 
        return  {
                    "data": {
                        "type": "person",
                        "id": id,
                        "name": name,
                        "activities": activities
                    }
                }

    except ItemNotFound as inf:
        print "Error: not found"

        response.status = 404 # "Not Found"
        return  {
                    "errors": [{
                        "not_found": {
                            "id": id
                        }
                    }]
                }