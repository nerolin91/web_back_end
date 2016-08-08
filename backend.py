#!/usr/bin/env python

'''
  Back end DB server for Assignment 3 and 4, CMPT 474.
'''

# Library packages
import argparse
import json
import sys
import time

# Installed packages
import boto.dynamodb2
import boto.dynamodb2.table
import boto.sqs

from bottle import request, response

# Local imports
import create_ops
import retrieve_ops
import delete_ops
import update_ops

AWS_REGION = "us-west-2"
TABLE_NAME_BASE = "activities"
Q_IN_NAME_BASE = "a3_back_in"
Q_OUT_NAME = "a3_out"

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60

def open_sqs_conn(region):
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
      sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
      sys.exit(1)
    return conn

def open_dynamodb_conn(region):
  conn = boto.dynamodb2.connect_to_region(AWS_REGION)
  if conn == None:
    sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
    sys.exit(1)
  return conn

def handle_args():
    argp = argparse.ArgumentParser(
        description="Backend for simple database")
    argp.add_argument('suffix', help="Suffix for queue base ({0}) and table base ({1})".format(Q_IN_NAME_BASE, TABLE_NAME_BASE))
    return argp.parse_args()


def writeTestRequestToInputQueue(seq_num):
  # Used to put a test message on the input queue that the
  #  while True loop below will pull off of.

  print("** WRITING TEST MESSAGE TO INPUT QUEUE ENABLED **")

  testMsg = boto.sqs.message.Message()
  msgBody = {}
  msgBody['msg_id'] = "testMsgId-OpnumIs" + str(seq_num)
  msgBody['opnum'] = seq_num

  # Uncomment only one of these (since same msg_id will result it in thinking its same request)
  # msgBody['jsonBody'] = {"action":"retrieve", "on":"users", "id":2222, "name":None}
  # msgBody['jsonBody'] = {"action":"retrieve", "on":"users", "id":None, "name":"Smith"}
  # msgBody['jsonBody'] = {"action":"add", "on":"users", "id":2222, "name":"Subir"}
  # msgBody['jsonBody'] = {"action":"delete", "on":"users", "id":2222, "name":None}
  # msgBody['jsonBody'] = {"action":"delete", "on":"users", "id":None, "name":"Subir"}
  # msgBody['jsonBody'] = {"action":"add", "on":"activity", "id":2222, "name":"Skiing"}
  # msgBody['jsonBody'] = {"action":"delete", "on":"activity", "id":2222, "name":"Skiing"}
  # msgBody['jsonBody'] = {"action":"delete", "on":"activity", "id":2222, "name":"Skiing"}
  msgBody['jsonBody'] = {"action":"get_list", "on":"users", "id":None, "name":None}

  testMsg.set_body(json.dumps(msgBody))

  inputQueue.write(testMsg)

def writeMsgToOutputQueue(returnResponse):
  outputMsg = boto.sqs.message.Message()
  outputMsg.set_body(json.dumps(returnResponse))
  outputQueue.write(outputMsg)

def processRequest(body, opnum):
  # Get and print the parameters from the JSON
  msg_id = body['msg_id']
  msg_body = body['jsonBody']
  request_action = msg_body['action']
  request_on = msg_body['on']
  request_id = msg_body['id']
  request_name = msg_body['name']

  print("Process Request For msg_id: " + msg_id)
  print("Action: {0}, On: {1}, ID: {2}, Name: {3}".format(request_action, request_on, request_id, request_name))


  # Check if the request is a duplicate (cached)
  if msg_id in seenRequests:
    print("\nDUPLICATE REQUEST: Passing back cached response\n")

    returnResponse = seenRequests[msg_id]

    writeMsgToOutputQueue(returnResponse)
    print(returnResponse)

  else:
    print("\nNON-DUPLICATE REQUEST: Hitting the database\n")

    httpResp = response # This creates the bottle.response object

    # Depending on whats requested follow the appropiate API path
    if request_action == "add" and request_on == "users" and request_id != None and request_name != None:
      print("Adding new user")
      requestResponse = create_ops.do_create(request, table, request_id, request_name, httpResp)

    elif request_action == "retrieve" and request_on == "users" and request_id != None and request_name == None:
      print("Retrieving user by ID")
      requestResponse = retrieve_ops.retrieve_by_id(table, request_id, httpResp)

    elif request_action == "retrieve" and request_on == "users" and request_id == None and request_name != None:
      print("Retrieving user by name")
      requestResponse = retrieve_ops.retrieve_by_name(table, request_name, httpResp)

    elif request_action == "delete" and request_on == "users" and request_id != None and request_name == None:
      print("Deleting user by id")
      requestResponse = delete_ops.delete_by_id(table, request_id, httpResp)

    elif request_action == "delete" and request_on == "users" and request_id == None and request_name != None:
      print("Deleting user by name")
      requestResponse = delete_ops.delete_by_name(table, request_name, httpResp)
    
    elif request_action == "add" and request_on == "activity" and request_id != None and request_name != None:
      print("Adding new activity")
      requestResponse = update_ops.add_activity(table, request_id, request_name, httpResp)
    
    elif request_action == "delete" and request_on == "activity" and request_id != None and request_name != None:
      print("Deleting activity")
      requestResponse = update_ops.delete_activity(table, request_id, request_name, httpResp)
    
    elif request_action == "get_list" and request_on == "users" and request_id == None and request_name == None:
      print("Getting list of users")
      requestResponse = retrieve_ops.retrieve_list(table, httpResp)
    
    else:
      print("INVALID JSON PARAMETERS")
      print(request_action)
      print(request_on)
      print(request_id)
      print(request_name)
      return


    # Construct the return response
    returnResponse = {}
    returnResponse['jsonBody'] = requestResponse
    returnResponse['httpStatusCode'] = httpResp.status_code # IE: 404, 200, etc
    returnResponse['msg_id'] = msg_id
    returnResponse['opnum'] = opnum

    seenRequests[msg_id] = returnResponse

    writeMsgToOutputQueue(returnResponse)
    print(returnResponse)
  # End If - Duplicate message check 


if __name__ == "__main__":
  args = handle_args()
  conn_sqs = open_sqs_conn(AWS_REGION)
  conn_dynamoDB = open_dynamodb_conn(AWS_REGION)
  inputQueue = None
  outputQueue = conn_sqs.create_queue(Q_OUT_NAME)
  table = None
  seenRequests = {}
  pendingList = {}
  lastOpnum = 0


  # Parse the input argument (Passed in as "$ ./backend.py _a" or "$ ./backend.py _b")
  if args.suffix == "_a" or args.suffix == "_b":
    inputQueue = conn_sqs.create_queue(Q_IN_NAME_BASE + args.suffix)
    table = boto.dynamodb2.table.Table(TABLE_NAME_BASE + args.suffix, connection=conn_dynamoDB)

    print("\nSuffix: " + "*" + args.suffix + "*")
    print("Input Queue: " + "*" + Q_IN_NAME_BASE + args.suffix + "*")
    print("DynamoDB Table: " + "*" + TABLE_NAME_BASE + args.suffix + "*\n") 
  else:
    sys.stderr.write("Invalid Arguement Suffix\n")
    sys.exit(1)  

  # Uncomment to add a test messages to the input queue: IMPORTANT: Comment out for production code
  #  NOTE: The order of these test requests aren't guarnteed, SQS code to write to queue seems to be multi-threaded
  # writeTestRequestToInputQueue(1)
  # writeTestRequestToInputQueue(3)
  # writeTestRequestToInputQueue(4)
  # writeTestRequestToInputQueue(2)
  # writeTestRequestToInputQueue(6)
  # writeTestRequestToInputQueue(5)
  # writeTestRequestToInputQueue(7)

  # Begin reading from the queue. Exit when timed out.
  wait_start = time.time()
  while True:
    print("\n-------------------------------------")
    print(" READING MESSAGE OFF THE INPUT QUEUE")
    msg_in = inputQueue.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
    if msg_in:
        # Delete message off the queue
        inputQueue.delete_message(msg_in)   

        # Get the body of the SQS message
        body = json.loads(msg_in.get_body())

        # Get opnum of message
        opnum_from_request = body['opnum']

        expectedOpnum = lastOpnum + 1

        # If we got the opnum we were expecting then continue as usual, otherwise stash the request.
        if expectedOpnum == opnum_from_request:
          print("  *Encountered EXPECTED Opnum: {0}".format(opnum_from_request))
          lastOpnum = opnum_from_request
          processRequest(body, opnum_from_request)

          # If we have any pending requests that were waiting for this opnum to come up; do them now.
          pendingOpnum = expectedOpnum + 1
          while pendingOpnum in pendingList:
            print("\n  *Processing PENDING request with opnum: {0}\n".format(pendingOpnum))
            processRequest(pendingList[pendingOpnum], pendingOpnum)
            # Update lastOpnum because we just fufilled a new request from the pending list
            lastOpnum = pendingOpnum
            del pendingList[pendingOpnum]
            # Increment so the while loop can check if the next request is pending as well
            pendingOpnum = pendingOpnum + 1

        elif opnum_from_request > expectedOpnum:
          print("Stashing Opnum: {0}".format(opnum_from_request))
          pendingList[opnum_from_request] = body
          #continue 
        
        wait_start = time.time()
    elif time.time() - wait_start > MAX_TIME_S:
        print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, Q_OUT_NAME)
        break
    else:
        pass  