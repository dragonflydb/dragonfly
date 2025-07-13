import socket
import ssl
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] %(message)s")


def main():
    server_host = "127.0.0.1"
    server_port = 6379

    # Path to CA bundle that verifies the server certificate
    ca_cert_path = "/path/to/ca-cert.pem"  # Replace with your CA bundle file

    # Client certificate and private key files (PEM format)
    client_cert_path = "path/to/cl-cert.pem"  # Client public certificate
    client_key_path = "path/to/cl-key.pem"  # Client private key

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)

    # Create an SSL context with CA and client certificates
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_path)
    context.load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)
    context.verify_mode = ssl.CERT_REQUIRED

    # Wrap the socket, but do NOT start handshake automatically
    ssl_sock = context.wrap_socket(sock, server_hostname=server_host, do_handshake_on_connect=False)

    try:
        ssl_sock.connect((server_host, server_port))
    except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
        pass  # Non-blocking connect will raise one of these

    ssl_sock.connect((server_host, server_port))
    logging.info("Starting TLS handshake, but will abort abruptly...")

    #    try:
    # Start handshake, but do not let it finish
    ssl_sock.do_handshake()
    logging.info("Handshake completed (unexpected for this test).")


#    except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
#        logging.info("Handshake in progress. Abruptly closing socket now!")
#        ssl_sock.close()
#        return
#    except ssl.SSLError as e:
#        logging.error(f"SSL error: {e}")
#        ssl_sock.close()
#        return
#
#    # If handshake somehow completes, close anyway
#    ssl_sock.close()

for i in range(1, 100000):
    try:
        main()
    except Exception as e:
        print(e)
        pass
