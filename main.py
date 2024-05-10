from http.server import BaseHTTPRequestHandler, HTTPServer
from connectors.snowflake import snowflake_ops
from connectors.mysql import mysql_ops
import json

class MainServer(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        self._set_response()
        response = {'message': 'Hello, worlds!'}
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        print(f"post data: {post_data}")

        if self.path == "/segregate-database":
            self.handle_segregate_database(post_data)
        else:
            self._set_response()
            response = {'error': 'Invalid endpoint'}
            self.wfile.write(json.dumps(response).encode('utf-8'))

    def handle_segregate_database(self, data):
        connector = data['connectorName'].lower()
        if connector == 'snowflake':
            self._set_response()
            conn = snowflake_ops.connect_to_snowflake(data['username'], data['password'], data['account'], data['warehouse'], data['database'], data['schema'])
            if conn is None:
                response = {'error': 'Failed to connect to Snowflake'}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

            table_data, error = snowflake_ops.get_tables_and_columns(conn, data['schema'], data['database'])
            snowflake_ops.close_connection(conn)

            if error:
                response = {'error': 'Query failed', 'details': error}
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                response = {data['schema']: table_data}
                self.wfile.write(json.dumps(response).encode('utf-8'))

        elif connector == 'mysql':
            self._set_response()
            conn = mysql_ops.connect_to_mysql(data['username'], data['password'], data['host'], data['database'])
            if conn is None:
                response = {'error': 'Failed to connect to MySQL'}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

            table_data, error = mysql_ops.get_tables_and_columns(conn, data['database'])
            mysql_ops.close_connection(conn)

            if error:
                response = {'error': 'Query failed', 'details': error}
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                response = {data['database']: table_data}
                self.wfile.write(json.dumps(response).encode('utf-8'))

        else:
            self._set_response()
            response = {'error': 'Unsupported database connector'}
            self.wfile.write(json.dumps(response).encode('utf-8'))

if __name__ == '__main__':
    server_address = ('', 8000)
    httpd = HTTPServer(server_address, MainServer)
    print("Server started at localhost:8000")
    httpd.serve_forever()
