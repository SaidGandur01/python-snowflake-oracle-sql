import os
import sys
import time
import subprocess

def file_has_changed(path, last_modified):
    try:
        return os.stat(path).st_mtime != last_modified
    except OSError:
        return False

def run_server():
    server_process = subprocess.Popen([sys.executable, 'main.py'])
    return server_process

def main():
    path = 'main.py'
    last_modified = os.stat(path).st_mtime

    server_process = run_server()

    try:
        while True:
            time.sleep(1)
            if file_has_changed(path, last_modified):
                last_modified = os.stat(path).st_mtime
                print("File changed, reloading server...")
                server_process.terminate()
                server_process.wait()
                server_process = run_server()
    except KeyboardInterrupt:
        print("Stopping server...")
        server_process.terminate()
        server_process.wait()

if __name__ == '__main__':
    main()
