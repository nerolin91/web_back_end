''' Add_activities function for CMPT 474 Assignment 2 '''

def add_activity(table, id, activity, response):
    print "Add activity not yet implemented"
    response.status = 501
    return {"errors": [{
        "add activity not implemented": {"id": id, "activity": activity}
        }]}
