'''
   Ops for the frontend of Assignment 3, Summer 2016 CMPT 474.
'''
import sys
# Standard library packages
import json

# Installed packages
import boto.sqs

# Imports of unqualified names
from bottle import post, get, put, delete, request, response

# Local modules
import SendMsg

# Constants
AWS_REGION = "us-west-2"

Q_IN_NAME_BASE = 'a3_in'
Q_OUT_NAME = 'a3_out'

# Respond to health check
@get('/')
def health_check():
    response.status = 200
    return "Healthy"

'''
# EXTEND:
# Define all the other REST operations here ...
@post('/users')
def create_route():
    pass
'''
@post('/users')
def create_route():
    ct = request.get_header('content-type')
    if ct != 'application/json':
        return abort(response, 400, [
            "request content-type unacceptable:  body must be "
            "'application/json' (was '{0}')".format(ct)])
    id = request.json["id"] # In JSON, id is already an integer
    name = request.json["name"]

    print "creating id {0}, name {1}\n".format(id, name)
    json = {"action":"add", "on":"users", "id":id, "name":name};
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);

@get('/users/<id>')
def get_id_route(id):
    id = int(id) # In URI, id is a string and must be made int
    print "Retrieving id {0}\n".format(id)
    json = {"action":"retrieve", "on":"users", "id":id, "name":None}
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);

    

@get('/names/<name>')
def get_name_route(name):
    print "Retrieving name {0}\n".format(name)
    json = {"action":"retrieve", "on":"users", "id":None, "name":name}
    json.dumps(json)
    result = send_msg_ob.send_msg(json, json);

    

@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)
    print "Deleting id {0}\n".format(id)
    json = {"action":"delete", "on":"users", "id":id, "name":None}
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);

    

@delete('/names/<name>')
def delete_name_route(name):
    json = {"action":"delete", "on":"users", "id":id, "name":None}
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);

    

@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    id = int(id)
    print "adding activity for id {0}, activity {1}\n".format(id, activity)
    json = {"action":"add", "on":"activity", "id":id, "name":activity}
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    
    

@delete('/users/<id>/activities/<activity>')
def delete_activity_route(id, activity):
    id = int(id)
    print "deleting activity for id {0}, activity {1}\n".format(id, activity)
    json = {"action":"delete", "on":"activity", "id":id, "name":activity}
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);

    
    
@get('/users')
def get_list_route():
    print "Retrieving users {0}\n".format(type, id)
    json = {"action":"get_list", "on":"users", "id":None, "name": None}
    json.dumps(json)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(json)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(json)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);

    


'''
   Boilerplate: Do not modify the following function. It
   is called by frontend.py to inject the names of the two
   routines you write in this module into the SendMsg
   object.  See the comments in SendMsg.py for why
   we need to use this awkward construction.

   This function creates the global object send_msg_ob.

   To send messages to the two backend instances, call

       send_msg_ob.send_msg(msg_a, msg_b)

   where 

       msg_a is the boto.message.Message() you wish to send to a3_in_a.
       msg_b is the boto.message.Message() you wish to send to a3_in_b.

       These must be *distinct objects*. Their contents should be identical.
'''
def set_send_msg(send_msg_ob_p):
    global send_msg_ob
    send_msg_ob = send_msg_ob_p.setup(write_to_queues, set_dup_DS)

'''
   EXTEND:
   Set up the input queues and output queue here
   The output queue reference must be stored in the variable q_out
'''

q_in_a = Q_IN_NAME_BASE + "_a";
q_in_b = Q_IN_NAME_BASE + "_b";
q_out = None
try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)

    # create_queue is idempotent---if queue exists, it simply connects to it
    q_in_a = conn.create_queue(q_in_a);
    q_in_b = conn.create_queue(q_in_b);
    q_out = conn.create_queue(q_out);
except Exception as e:
    sys.stderr.write("Exception connecting to SQS\n")
    sys.stderr.write(str(e) + "\n")
    sys.exit(1)


def write_to_queues(msg_a, msg_b):
    msg_a_id = q_in_a.send_message(msg_a);
    msg_b_id = q_in_b.send_message(msg_b);


'''
   EXTEND:
   Manage the data structures for detecting the first and second
   responses and any duplicate responses.
'''

# Define any necessary data structures globally here
firstResponseId=[]
secondResPonseId=[]
pairId=[]
partnerList=[]

def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    if id not in firstResponseId:
        if id in pairId and len(pariId)>1:
            return True
        else:
            return False   #it's 2nd response response.
    else:
        return False #It's dulplicate response.
    pass

def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    if id not in secondResPonseId(id):
      if id in pairId:
        return True
      else:
        return False
    else:
        return False   #It's dulplicate response
    pass

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    partnerList=pariId
    if is_first_response:
      pairId.pop(pairId.index[id])
    if is_second_response:
      pairId.pop(pairId.index[id])
    pass

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    partnerId=partnerList[partnerList.index(id)-1]
    return partnerId
    pass

def mark_first_response(id):
    # EXTEND:
    # Update the data structures to note that the first response has been received
    firstResponseId.append(id)
    pass

def mark_second_response(id):
    # EXTEND:
    # Update the data structures to note that the second response has been received
    secondResPonseId.append(id)
    pass

def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
    pairId=[]  #empty the pairId list
    pass

def set_dup_DS(action, sent_a, sent_b):
    '''
       EXTEND:
       Set up the data structures to identify and detect duplicates
       action: The action to perform on receipt of the response.
               Opaque data type: Simply save it, do not interpret it.
       sent_a: The boto.sqs.message.Message() that was sent to a3_in_a.
       sent_b: The boto.sqs.message.Message() that was sent to a3_in_b.
       
               The .id field of each of these is the message ID assigned
               by SQS to each message.  These ids will be in the
               msg_id attribute of the JSON object returned by the
               response from the backend code that you write.
    '''
 
    msg_a = json.loads(sent_a.get_body())
    msg_a = json.loads(sent_b.get_body())
    msg_a_id = msg_a['msg_id']
    msg_b_id = msg_b['msg_id']
    pairId.append(msg_a_id)
    pairId.append(msg_b_id)

    pass
