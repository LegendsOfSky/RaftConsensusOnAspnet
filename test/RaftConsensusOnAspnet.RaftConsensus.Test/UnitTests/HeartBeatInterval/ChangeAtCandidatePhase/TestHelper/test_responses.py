import uuid
from request_data_types import AppendEntriesReply , VoteRequestReply


DEFAULT_REPLIER_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")


patch_vote_replies : list[VoteRequestReply] = []
put_entries_replies : list[AppendEntriesReply] = []
