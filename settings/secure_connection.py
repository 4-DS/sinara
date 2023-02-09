import os
from urllib.parse import urlparse

def check_secure_connection():
    is_secure = False
    if '1' == os.environ.get('CHECK_SECURE_CONNECTION', '1'):
        if os.path.exists("/tmp/jupyter_server_url"):
            with open("/tmp/jupyter_server_url", "r") as url_file:
                host_url = url_file.read().strip()
                parsed_uri = urlparse(host_url)
                if 'https' == parsed_uri.scheme or parsed_uri.netloc.split(':')[0] in ['localhost', '127.0.0.1']:
                    is_secure = True
        else:
            from jupyter_server import serverapp
            server_info = next(serverapp.list_running_servers())
            is_secure = server_info['secure']
        if not is_secure:
            raise Exception('Sinara Server connection is not secure!')
