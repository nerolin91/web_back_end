#!/usr/bin/env python

'''
  Back end DB server for Assignment 3, CMPT 474.
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

from bottle import response

# Local imports
# import create_ops
import retrieve_ops
# import delete_ops
# import update_ops

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


def writeTestRequestToInputQueue():
  # Uncomment this block to put a test message on the input queue that the
  #  while True loop below will pull off of.
  testMsg = boto.sqs.message.Message()
  msgBody = {}
  msgBody['msg_id'] = "fejd392hdewdvcca"
  msgBody['req'] = {"action":"retrieve", "on":"users", "data":18}

  testMsg.set_body(json.dumps(msgBody))

  inputQueue.write(testMsg)


if __name__ == "__main__":
  args = handle_args()
  conn_sqs = open_sqs_conn(AWS_REGION)
  conn_dynamoDB = open_dynamodb_conn(AWS_REGION)
  inputQueue = None
  outputQueue = conn_sqs.create_queue(Q_OUT_NAME)
  table = None
  seenRequests = {}


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

  writeTestRequestToInputQueue()

  # Begin reading from the queue. Exit when timed out.
  wait_start = time.time()
  while True:
    print("\n-------------------------------------")
    print(" READING MESSAGE OFF THE INPUT QUEUE")
    msg_in = inputQueue.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
    if msg_in:
        body = json.loads(msg_in.get_body())
        msg_id = body['msg_id']


        # Get and print the parameters from the JSON
        msg_body = body['req']
        request_action = msg_body['action']
        request_on = msg_body['on']
        request_data = msg_body['data']

        print("Process Request For msg_id: " + msg_id)
        print("Action: " + request_action)
        print("On:     " + request_on)
        #print("Data:   " + request_data + "\n")


        # Check if the request is a duplicate (cached)
        if msg_id in seenRequests:
          print("\nDUPLICATE REQUEST: Passing back cached response\n")

          returnResponse = seenRequests[msg_id]
          
          # outputQueue.write(returnResponse)
          print(returnResponse)

        else:
          print("\nNON-DUPLICATE REQUEST: Hitting the database\n")

          httpResp = response # This creates the bottle.response object
          requestResponse = retrieve_ops.retrieve_by_id(table, request_data, httpResp)

          # Construct the return response
          returnResponse = {}
          returnResponse['jsonBody'] = requestResponse
          returnResponse['httpStatusCode'] = httpResp.status_code
          returnResponse['msg_id'] = msg_id

          seenRequests[msg_id] = returnResponse

          # outputQueue.write(returnResponse)
          print(returnResponse)


        wait_start = time.time()
    elif time.time() - wait_start > MAX_TIME_S:
        print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
        break
    else:
        pass  


'''
      ALMOST DONE!
      ------------
       EXTEND:
       
       After the above statement, args.suffix holds the suffix to use
       for the input queue and the DynamoDB table.

       This main routine must be extended to:
       1. Connect to the appropriate queues
       2. Open the appropriate table
       3. Go into an infinite loop that
          - reads a requeat from the SQS queue, if available
          - handles the request idempotently if it is a duplicate
          - if this is the first time for this request
            * do the requested database operation
            * record the message id and response
            * put the response on the output queue
'''
