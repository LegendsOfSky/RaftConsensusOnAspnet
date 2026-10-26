import json
import os
import output_helper
import uuid
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from request_reply_helper import AppendEntriesReply , VoteRequestReply , to_payload


TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
PATH_APPEND_ENTRIES_REQUEST = "/api/node/entries"
PATH_VOTE_REQUEST = "/api/node/vote"
PIPE_NOTIFICATION_CHANNEL = "/tmp/notification_channel"
QUERY_REQUEST_ID = "requestId"
QUERY_REQUESTER_ID = "requesterId"
QUERY_REQUESTER_TERM = "requesterTerm"
QUERY_REQUESTER_LAST_LOG_TERM = "requesterLastLogTerm"
QUERY_REQUESTER_LAST_LOG_INDEX = "requesterLastLogIndex"
REPLIER_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")

put_entries_replies = []
patch_vote_replies = []

class RequestHandler(BaseHTTPRequestHandler):
    pipe = None

    def __del__(self):
        os.close(self.pipe)

    def log_message(self , format , *args):
        # Custom logging
        output_helper.log(f"[{datetime.now()}] {self.address_string()} - {format % args}")

    def do_PUT(self):
        self.create_test_output_from_current_request()
        path, query_parameters, request_body = self.get_request_info()
        response_body = ""
        if path == PATH_APPEND_ENTRIES_REQUEST:
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
        self.create_test_output_from_current_request()
        path, query_parameters, request_body = self.get_request_info()
        response_body = ""
        if path == PATH_VOTE_REQUEST:
            if len(patch_vote_replies) == 0:
                request_id = uuid.UUID(query_parameters[QUERY_REQUEST_ID])
                receiver_id = uuid.UUID(query_parameters[QUERY_REQUESTER_ID])
                term_of_request = query_parameters[QUERY_REQUESTER_TERM]
                reply = VoteRequestReply(request_id , REPLIER_ID , receiver_id , term_of_request , term_of_request , False)
            else:
                reply = patch_vote_replies.pop()
            response_body = json.dumps(to_payload(reply) , indent=2)
        self._send_response(200 , response_body.encode())
        self.send_notification(datetime.strftime(datetime.now(), TIME_FORMAT))

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

    def create_test_output_from_current_request(self):
        content_length = int(self.headers['Content-Length'])
        request_body = self.rfile.read(content_length)
        output_helper.test_output(f"{datetime.strftime(datetime.now(), TIME_FORMAT)}---{self.path},,,<{str(request_body)}>")

    def send_notification(self , msg):
        if self.pipe is None:
            try:
                os.mkfifo(PIPE_NOTIFICATION_CHANNEL)
            except FileExistsError:
                pass
            self.pipe = os.open(PIPE_NOTIFICATION_CHANNEL , os.O_WRONLY | os.O_NONBLOCK)
        payload = msg.encode()
        os.write(self.pipe, f"{len(payload):3d}".encode())
        os.write(self.pipe, msg.encode())

    @staticmethod
    def extract_info_from_test_output(test_output):
        try:
            time , rest = test_output.split("---" , maxsplit=1)
            path_and_parameters , request_body = rest.split(",,," , maxsplit=1)
            path , url_parameters = path_and_parameters.split("?" , maxsplit=1)
            query_parameters = {}
            for param in url_parameters.split("&"):
                segments = param.split("=")
                key, value = segments[0], segments[1]
                query_parameters[key] = value
            return True , datetime.strptime(time , TIME_FORMAT) , path , query_parameters , "" if len(request_body) <= 2 else request_body[1:-1]
        except ValueError:
            return False , None , None , None , None


def main(host="0.0.0.0", port=7195):
    global handler_started
    server = HTTPServer((host , port) , RequestHandler)
    output_helper.log(f"Listening on http://{host}:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()
