''' Retrieve operations for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

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
		# print "retrieve_by_id function - Error: No id found"

		response.status = 404 # "Not Found"
		return  {
					"errors": [{
						"not_found": {
							"id": id
						}
					}]
				}

def retrieve_by_name(table, name, response):
	try:
		# result = table.scan(name__eq=name)
		result = table.scan()
		no_user = True

		for user in result:
			if user["name"] != name:
				continue

			no_user = False

			userID = user["id"]
			userName = user["name"]
			activities = user["activities"]

			if activities is None:
				activities = []

			response.status = 200 
			return  {
						"data": {
							"type": "person",
							"id": int(userID),
							"name": userName,
							"activities" : activities
						}
					}

		if no_user:
			raise ItemNotFound

	except ItemNotFound as inf:
		response.status = 404 # "Not Found"
		return 	{
					"errors": [{
						"not_found": {
						"name": name
						}
					}]
				}

