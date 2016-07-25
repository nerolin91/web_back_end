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

AWS_REGION = "us-west-2"
TABLE_NAME_BASE = "activities"
Q_IN_NAME_BASE = "a3_back_in"
Q_OUT_NAME = "a3_out"

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60

def open_conn(region):
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)
    return conn

def handle_args():
    argp = argparse.ArgumentParser(
        description="Backend for simple database")
    argp.add_argument('suffix', help="Suffix for queue base ({0}) and table base ({1})".format(Q_IN_NAME_BASE, TABLE_NAME_BASE))
    return argp.parse_args()

if __name__ == "__main__":
  args = handle_args()
  conn = open_conn(AWS_REGION)
  inputQueue = None
  outputQueue = conn.create_queue(Q_OUT_NAME)
  table = None


  if args.suffix == "_a" or args.suffix == "_b":
    inputQueue = conn.create_queue(Q_IN_NAME_BASE + args.suffix)
    table = boto.dynamodb2.table.Table(TABLE_NAME_BASE + args.suffix, connection=conn)
    print("\nSuffix: " + "*" + args.suffix + "*")
    print("Input Queue: " + "*" + Q_IN_NAME_BASE + args.suffix + "*")
    print("DynamoDB Table: " + "*" + TABLE_NAME_BASE + args.suffix + "*\n") 
  else:
    sys.stderr.write("Invalid Arguement Suffix\n")
    sys.exit(1)  

  wait_start = time.time()
  while True:
    msg_in = inputQueue.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
    if msg_in:
        body = json.loads(msg_in.get_body())
        msg_id = body['msg_id']

        print("Process Message")

        wait_start = time.time()
    elif time.time() - wait_start > MAX_TIME_S:
        print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
        break
    else:
        pass  




''' 
  if conn == None:
    sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
    sys.exit(1)

    outputQueue = conn.create_queue("a3_out")

    print("HIT 1")

    if args.suffix == "_a":
      queue = conn.create_queue("a3_back_in_a")
      table = boto.dynamodb2.table.Table("activities_a", connection=conn)       
    if args.suffix == "_b":
      queue = conn.create_queue("a3_back_in_b")
      table = boto.dynamodb2.table.Table("activities_b", connection=conn)
      print("HIT 2")

    while True:
      messages = queue.get_messages()
      dictionary = {}

      for message in messages:
        msgID = message["msg_id"]

        if msgID in dictionary:
          outputQueue.write(dictionary[msgID])
        else:
          result = None
          #hit database using "message" variable
          #perform whatever needs to be done to fufil request, store value in result
          if message["action"] == "create":
            print("action create msg")

          if message["action"] == "delete":
            print("action delete msg")

          if message["action"] == "retrieve":
            print("action retrieve msg")

          dictionary[msgID] = result;
      
      time.sleep(1)
'''
'''
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
