import json

from .client import Client

HOST = "127.0.0.1"
PORT = 4000

def main():
    client = Client(HOST, PORT)
    # push multiple jobs over the SAME connection
    

    # fetch them back
    for i in range(500):
        print(client.ack_job(f"job{i}"))

    client.close()

if __name__ == "__main__":
    main()
