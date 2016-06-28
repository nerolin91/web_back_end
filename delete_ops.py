''' Delete operations for Assignment 2 of CMPT 474 '''

# Installed packages
from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound



def delete_by_id(table, id, response):
    try:
        item = table.get_item(id=id)
        table.delete_item(id=id)
        item.partial_save()
        response.status = 200
        return {
            "data": {
                "type": "person",
                "id": id
            }
        }
    except ItemNotFound as inf:
        response.status = 404
        return {
            "errors":[{
                "not_found":{
                "id": id 
                }
            }]
        }





def delete_by_name(table, name, response):
    no_user=True
    # scan whole table
    all_users= table.scan() 
    for user in all_users:
        if user["name"]==name:
            no_user=False
            user.delete()
            # save the deleting result
            user.partial_save()
            response.status = 200
            return {
                        "data": {
                            "type": "person",
                            "id": int(user["id"])
                        }
                    }
    if no_user:
        response.status = 404
        return {
            "errors": [{
                "not_found": {
                "name": name
                }
            }]
        }



# def delete_by_name(table, name, response):

#     try:
#         # scan whole table
#         all_users= table.scan() 
#         for user in all_users:
#             if user["name"]==name:
                
#                 user.delete()
#                 # save the deleting result
#                 user.partial_save()
#         response.status = 200
#         return {
#                     "data": {
#                         "type": "person",
#                         "id": int(user["id"])
#                     }
#                 }
                
        
#     except ItemNotFound as inf:
#         response.status = 404
#         return {
#             "errors": [{
#                 "not_found": {
#                 "name": name
#                 }
#             }]
#         }