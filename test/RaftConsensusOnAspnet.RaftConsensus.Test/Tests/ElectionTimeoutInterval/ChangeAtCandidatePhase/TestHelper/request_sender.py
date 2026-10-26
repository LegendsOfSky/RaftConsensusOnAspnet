import requests
import urllib.parse
from request_reply_helper import to_payload


def stop_node(ip):
    requests.delete(f"{ip}/api/node")

def patch_vote(ip , request_id , requester_id , requester_term , requester_last_log_term , requester_last_log_index):
    params = {
        'requestId': request_id ,
        'requesterId': requester_id ,
        'requesterTerm': requester_term ,
        'requesterLastLogTerm': requester_last_log_term ,
        'requesterLastLogIndex': requester_last_log_index
    }
    requests.patch(f"{ip}/api/node/vote?{urllib.parse.urlencode(params)}")

def put_entries(ip , request_id , requester_id , requester_term , previous_log_index , previous_log_term , leader_commit , entries):
    params = {
        'requestId': request_id ,
        'requesterId': requester_id ,
        'requesterTerm': requester_term ,
        'previousLogIndex': previous_log_index ,
        'previousLogTerm': previous_log_term ,
        'leaderCommit': leader_commit
    }
    payload_entries = [to_payload(entry) for entry in entries]
    requests.put(f"{ip}/api/node/entries?{urllib.parse.urlencode(params)}" , json=payload_entries)

def post_heart_beat_interval(ip , interval):
    params = {'newValue': interval}
    requests.post(f"{ip}/api/node/heart-beat-interval?{urllib.parse.urlencode(params)}")

def post_election_timeout_interval(ip , interval):
    params = {'newValue': interval}
    requests.post(f"{ip}/api/node/election-timeout-interval?{urllib.parse.urlencode(params)}")
