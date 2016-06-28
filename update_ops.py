''' Add_activities function for CMPT 474 Assignment 2 '''
from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound


def definition():
	global data
	global type
	global constid
	global added
	global deleted
	global type_person
	data = "data".encode("utf-8")
	type = "type".encode("utf-8")
	constid = "id".encode("utf-8")
	added = "added".encode("utf-8")
	deleted = "deleted".encode("utf-8")
	type_person = "person".encode("utf-8")
	
def add_activity(table, id, activity, response):
	definition()
	try:
		item = table.get_item(id=id)
		activities = item["activities"]
		if activities is None:
			activities = []
		activity = activity.replace(" ", "_")
		if any(activity in s for s in activities):
			activity = []
		else:
			activities.append(activity)
			activity = [activity]

		item["activities"] = activities

		item.partial_save()


		return {data: {
        	type: type_person,
        	constid: id,	
        	added: activity
        }
    }
	except ItemNotFound as inf:
		return {
  					"errors": [{
    					"not_found": {
       							"id": id
      					}
  					}]
		}

def delete_activity(table, id, activity, response):
	definition()
	try:
		item = table.get_item(id=id)
		activities = item["activities"]
		if activities is None:
			activities = []
		activity = activity.replace(" ", "_")
		if any(activitys in s for s in activities):
			activities.remove(activity)
			activity = [activity]
		else:
			activity = []

		item["activities"] = activities

		item.partial_save()

		return {data: {
        	type: type_person,
        	constid: id,	
        	deleted: activity
        }
    }
	except ItemNotFound as inf:
		return {
  					"errors": [{
    					"not_found": {
       							"id": id
      					}
  					}]
		}