import json
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from RequestReplyHelper import AppendEntriesReply , VoteRequestReply , to_payload


handler_started = False
put_entries_replies = []
patch_vote_replies = []

class RequestHandler(BaseHTTPRequestHandler):
    def log_message(self , format , *args):
        # Custom logging
        print(f"[{self.log_date_time_string()}] {self.address_string()} - {format % args}")

    def _send_response(self , code, body , content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type" , content_type)
        self.send_header("Content-Length" , str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        """
        /api/node/entries
        """
        if len(put_entries_replies) == 0:
            reply = AppendEntriesReply(uuid.uuid4() , uuid.uuid4() , uuid.uuid4() , 0 ,False , 0)
        else:
            reply = put_entries_replies.pop()
        body = json.dumps(to_payload(reply) , indent=2).encode()
        self._send_response(200 , body)

    def do_PATCH(self):
        """
        /api/node/vote
        """
        if len(patch_vote_replies) == 0:
            reply = VoteRequestReply(uuid.uuid4() , uuid.uuid4() , uuid.uuid4() , 0 , 0 , False)
        else:
            reply = patch_vote_replies.pop()
        body = json.dumps(to_payload(reply) , indent=2).encode()
        self._send_response(200 , body)

def start_handler(host="0.0.0.0", port=7195):
    global handler_started
    server = HTTPServer((host , port) , RequestHandler)
    print(f"Listening on http://{host}:{port}")
    try:
        handler_started = True
        while handler_started:
            server.handle_request()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

def stop_handler():
    global handler_started
    handler_started = False
