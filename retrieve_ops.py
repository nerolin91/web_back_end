''' Retrieve operations for CMPT 474 Assignment 2 '''

def retrieve_by_id(table, id, response):
    print "Retrieve by id not yet implemented"
    response.status = 501
    return {"errors": [{
        "retrieve by id not implemented": {"id": id}
        }]}
