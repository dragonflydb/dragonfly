import ssl
import socket


def main():
    # Paths to your certificate files
    CA_CERT = "/ca-cert.pem"  # Replace with your CA bundle file

    CLIENT_CERT = "/cl-cert.pem"  # Client public certificate
    CLIENT_KEY = "/cl-key.pem"  # Client private key

    # Create a single SSL context to be reused for all connections
    # context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_CERT)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_CERT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.maximum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False

    session = None
    host = "127.0.0.1"
    port = 6379  # Use your TLS port
    for i in range(1, 1000000000):
        sock = socket.create_connection((host, port))
        ssl_sock = context.wrap_socket(sock, server_hostname=host)
        print(f"TLS session reused: {ssl_sock.session_reused}")
        session = ssl_sock.session


main()
