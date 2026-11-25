import json
import requests
import urllib.parse
from request_data_types import AppendEntriesReply , VoteRequestReply , to_payload


def stop_node(ip):
    requests.delete(f"{ip}/api/node")
    return


def patch_vote(ip , request_id , requester_id , requester_term , requester_last_log_term , requester_last_log_index):
    params = {
        'requestId': request_id ,
        'requesterId': requester_id ,
        'requesterTerm': requester_term ,
        'requesterLastLogTerm': requester_last_log_term ,
        'requesterLastLogIndex': requester_last_log_index
    }
    response = requests.patch(f"{ip}/api/node/vote?{urllib.parse.urlencode(params)}")
    response_content = json.loads(response.text)
    reply = VoteRequestReply(
            response_content['requestId'] , response_content['replierId'] , response_content['receiverId'] , response_content['replierTerm'] ,
            response_content['termOfRequest'] , response_content['voteGranted']
        )
    return reply


def patch_entries(ip, request_id, requester_id, requester_term, previous_log_index, previous_log_term, leader_commit, entries):
    params = {
        'requestId': request_id ,
        'requesterId': requester_id ,
        'requesterTerm': requester_term ,
        'previousLogIndex': previous_log_index ,
        'previousLogTerm': previous_log_term ,
        'leaderCommit': leader_commit
    }
    payload_entries = [to_payload(entry) for entry in entries]
    response = requests.patch(f"{ip}/api/node/entries?{urllib.parse.urlencode(params)}" , json=payload_entries)
    response_content = json.loads(response.text)
    reply = AppendEntriesReply(
            response_content['requestId'] , response_content['replierId'] , response_content['receiverId'] , response_content['replierTerm'] ,
            response_content['appendSuccess'] , response_content['matchIndex']
        )
    return reply


def post_heart_beat_interval(ip , interval):
    params = {'newValue': interval}
    requests.post(f"{ip}/api/node/heart-beat-interval?{urllib.parse.urlencode(params)}")
    return


def post_election_timeout_interval(ip , interval):
    params = {'newValue': interval}
    requests.post(f"{ip}/api/node/election-timeout-interval?{urllib.parse.urlencode(params)}")
    return
