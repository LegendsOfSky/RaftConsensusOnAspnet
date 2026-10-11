import json
from http.server import HTTPServer, BaseHTTPRequestHandler


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
        content_length = int(self.headers.get("Content-Length" , 0))
        body_bytes = self.rfile.read(content_length)
        try:  # Try to parse as JSON, fall back to raw.
            body_data = json.loads(body_bytes)
        except json.JSONDecodeError:
            body_data = body_bytes.decode("utf-8" , errors="replace")
        response = {
            "method": "POST" ,
            "path": self.path ,
            "headers": dict(self.headers) ,
            "body": body_data
        }
        body = json.dumps(response , indent=2).encode()
        self._send_response(200 , body)

    def do_PATCH(self):
        """
        /api/node/vote
        """
        self.do_PUT()  # treat similarly for demo

def start_handler(host="0.0.0.0", port=7195):
    server = HTTPServer((host , port) , RequestHandler)
    print(f"Listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()