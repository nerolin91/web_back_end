''' Delete operations for Assignment 2 of CMPT 474 '''

# Installed packages

from boto.dynamodb2.exceptions import ItemNotFound

def delete_by_id(table, id, response):
    print "Delete by id not yet implemented"
    response.status = 501
    return {"errors": [{
        "delete by id not implemented": {"id": id}
        }]}
