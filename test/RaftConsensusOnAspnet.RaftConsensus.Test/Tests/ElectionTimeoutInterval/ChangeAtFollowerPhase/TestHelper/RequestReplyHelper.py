import uuid
from dataclasses import dataclass , asdict
from enum import Enum
from typing import Optional

@dataclass
class AppendEntriesReply:
    RequestId : uuid.UUID
    ReplierId : uuid.UUID
    ReceiverId : uuid.UUID
    ReplierTerm : int
    AppendSuccess : bool
    MatchIndex : int

@dataclass
class VoteRequestReply:
    RequestId : uuid.UUID
    ReplierId : uuid.UUID
    ReceiverId : uuid.UUID
    ReplierTerm : int
    TermOfRequest : int
    VoteGranted : bool

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


def to_payload(item: LogEntries | AppendEntriesReply | VoteRequestReply) -> dict:
    d = asdict(item)
    for k, v in d.items():
        if isinstance(v, uuid.UUID):
            d[k] = str(v)
    return d
