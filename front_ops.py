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
# Operation counter
seq_num=0
# Respond to health check
@get('/')
def health_check():
    response.status = 1
    return "Healthy"

'''
# EXTEND:
# Define all the other REST operations here ...
@post('/users')
def create_route():
    pass
'''
def make_response(result):
    response.status = result['httpStatusCode'];
    response.body = json.dumps(result['jsonBody']);
    return result;

@post('/users')
def create_route():
    ct = request.get_header('content-type')
    if ct != 'application/json':
        return "request content-type unacceptable"
    id = request.json["id"] # In JSON, id is already an integer
    name = request.json["name"]

    print "creating id {0}, name {1}\n".format(id, name)
    global seq_num
    #increase operation counter when route created
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"add", "on":"users", "id":id, "name":name};
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg);
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);

@get('/users/<id>')
def get_id_route(id):
    id = int(id) # In URI, id is a string and must be made int
    print "Retrieving id {0}\n".format(id)
    # increase operation counter while retrieving user id
    global seq_num;
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"retrieve", "on":"users", "id":id, "name":None}
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from~
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);



@get('/names/<name>')
def get_name_route(name):
    print "Retrieving name {0}\n".format(name)
    # increase operation counter while retrieving by user name
    global seq_num
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"retrieve", "on":"users", "id":None, "name":name}
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from~
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);



@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)
    print "Deleting id {0}\n".format(id)
    # increase operation counter while deleting by user id
    global seq_num
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"delete", "on":"users", "id":id, "name":None};
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);



@delete('/names/<name>')
def delete_name_route(name):
    # increase operation counter while deleting by user name
    global seq_num
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"delete", "on":"users", "id":None, "name":name}
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);



@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    id = int(id)
    print "adding activity for id {0}, activity {1}\n".format(id, activity)
    # increase operation counter while adding activity
    global seq_num
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"add", "on":"activity", "id":id, "name":activity}
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);



@delete('/users/<id>/activities/<activity>')
def delete_activity_route(id, activity):
    id = int(id)
    print "deleting activity for id {0}, activity {1}\n".format(id, activity)
    # increase operation counter while deleting activity
    global seq_num
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"delete", "on":"activity", "id":id, "name":activity}
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);



@get('/users')
def get_list_route():
    print "Retrieving users {0}\n".format(type, id)
    global seq_num
    # increase operation counter while retrieving all users
    seq_num+=1;
    msg = {};
    msg["jsonBody"] = {"action":"get_list", "on":"users", "id":None, "name": None}
    # place seq_num value as key 'opnum' in the message
    msg["opnum"] = seq_num.value;
    msg = json.dumps(msg)
    msg_a = boto.sqs.message.Message()
    msg_a.set_body(msg)
    msg_b = boto.sqs.message.Message()
    msg_b.set_body(msg)
    # Pass the called routine the response object to construct a response from
    result = send_msg_ob.send_msg(msg_a, msg_b);
    return make_response(result);

def setup_op_counter():
    # zookeeper operation counter
    global seq_num
    zkcl = send_msg_ob.get_zkcl()
    if not zkcl.exists('/SeqNum'):
        zkcl.create('/SeqNum', "0")
    else:
        zkcl.set('/SeqNum', "0")

    seq_num = zkcl.Counter('/SeqNum')


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
q_out = Q_OUT_NAME
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
    msg_a_id = q_in_a.write(msg_a);
    msg_b_id = q_in_b.write(msg_b);

'''
   EXTEND:
   Manage the data structures for detecting the first and second
   responses and any duplicate responses.
'''
# Define any necessary data structures globally here
firstResponseId=[]
secondResponseId=[]
pairId={}
partnerList={}


def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    if id not in firstResponseId:
        if id not in secondResponseId:
            return True;
    return False;

def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    if id in firstResponseId:
        if id not in secondResponseId:
            return True;
    return False;

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    return pairId.get(id);

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    return partnerList.get(id);
    pass

def mark_first_response(id):
    # EXTEND:
    # Update the data structures to note that the first response has been received
    firstResponseId.append(id)
    pass

def mark_second_response(id):
    # EXTEND:
    # Update the data structures to note that the second response has been received
    secondResponseId.append(id)
    pass

def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
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
    msg_b = json.loads(sent_b.get_body())
    msg_a_id = sent_a.id
    msg_b_id = sent_b.id
    pairId[msg_a_id] = action;
    pairId[msg_b_id] = action;
    ##to get partner message id
    partnerList[msg_a_id] = msg_b_id;
    partnerList[msg_b_id] = msg_a_id;

    pass
