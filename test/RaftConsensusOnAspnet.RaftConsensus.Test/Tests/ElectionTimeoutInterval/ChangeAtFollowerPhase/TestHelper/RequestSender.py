import requests
import urllib.parse
import uuid
from dataclasses import dataclass , asdict
from enum import Enum
from typing import Optional


class Operation(str , Enum):
    NO_OP = "None"
    PUT = "Put"
    DELETE = "Delete"

@dataclass
class LogEntries:
    guid : uuid.UUID
    logType : str
    term : int
    operation : Operation
    key : Optional[str] = None
    value : Optional[str] = None


def to_payload(item: LogEntries) -> dict:
    d = asdict(item)
    for k, v in d.items():
        if isinstance(v, uuid.UUID):
            d[k] = str(v)
    return d

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
