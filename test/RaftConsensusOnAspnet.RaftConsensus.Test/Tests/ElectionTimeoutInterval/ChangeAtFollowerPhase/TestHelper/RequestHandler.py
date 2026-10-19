import json
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from RequestReplyHelper import AppendEntriesReply , VoteRequestReply , to_payload


QUERY_REQUEST_ID = "requestId"
QUERY_REQUESTER_ID = "requesterId"
QUERY_REQUESTER_TERM = "requesterTerm"
QUERY_REQUESTER_LAST_LOG_TERM = "requesterLastLogTerm"
QUERY_REQUESTER_LAST_LOG_INDEX = "requesterLastLogIndex"
REPLIER_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")

put_entries_replies = []
patch_vote_replies = []

class RequestHandler(BaseHTTPRequestHandler):
    def log_message(self , format , *args):
        # Custom logging
        print(f"[{self.log_date_time_string()}] {self.address_string()} - {format % args}")

    def do_PUT(self):
        APPEND_ENTRIES_REQUEST_PATH = "/api/node/entries"
        path, query_parameters, request_body = self.get_request_info()
        for param in query_parameters:
            print(f"{param} => {query_parameters[param]}")
        response_body = ""
        content_length = int(self.headers['Content-Length'])
        print(f"length = {content_length} data = {self.rfile.readlines()}")
        if path == APPEND_ENTRIES_REQUEST_PATH:
            if len(put_entries_replies) == 0:
                request_id = uuid.UUID(query_parameters[QUERY_REQUEST_ID])
                receiver_id = uuid.UUID(query_parameters[QUERY_REQUESTER_ID])
                term_of_request = query_parameters[QUERY_REQUESTER_TERM]
                reply = AppendEntriesReply(request_id , REPLIER_ID , receiver_id , term_of_request ,False , 0)
            else:
                reply = put_entries_replies.pop()
            response_body = json.dumps(to_payload(reply), indent=2).encode()
        self._send_response(200 , response_body)

    def do_PATCH(self):
        VOTE_REQUEST_PATH = "/api/node/vote"
        path, query_parameters, request_body = self.get_request_info()
        response_body = ""
        if path == VOTE_REQUEST_PATH:
            if len(patch_vote_replies) == 0:
                request_id = uuid.UUID(query_parameters[QUERY_REQUEST_ID])
                receiver_id = uuid.UUID(query_parameters[QUERY_REQUESTER_ID])
                term_of_request = query_parameters[QUERY_REQUESTER_TERM]
                reply = VoteRequestReply(request_id , REPLIER_ID , receiver_id , term_of_request , term_of_request , False)
            else:
                reply = patch_vote_replies.pop()
            response_body = json.dumps(to_payload(reply) , indent=2)
        self._send_response(200 , response_body.encode())

    def _send_response(self , code, body , content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type" , content_type)
        self.send_header("Content-Length" , str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def get_request_info(self):
        url_segments = self.path.split("?" , maxsplit=1)
        path = url_segments[0]

        query_parameters = {}
        for param in url_segments[1].split("&"):
            segments = param.split("=")
            key , value = segments[0] , segments[1]
            query_parameters[key] = value

        content_length = int(self.headers['Content-Length'])
        request_body = self.rfile.read(content_length)

        return path , query_parameters , request_body

def main(host="0.0.0.0", port=7195):
    global handler_started
    server = HTTPServer((host , port) , RequestHandler)
    print(f"Listening on http://{host}:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()
