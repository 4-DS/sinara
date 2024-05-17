import atexit
import os
import socket

from abc import (
  ABC,
  abstractmethod,
)
class _SinaraSpark(ABC):    
    @staticmethod
    @abstractmethod
    def session_is_stopped():
        pass

    
    @staticmethod
    @abstractmethod
    def run_session(clusterSize=0 , app="my app", conf=None, reuse_session=True):
        pass

    
    @staticmethod
    @abstractmethod
    def ui_url():
        pass
    
    
    @staticmethod
    @abstractmethod
    def stop_session():
        pass
    
    
    @staticmethod
    @abstractmethod
    def _conf(conf = None, driver_mem = "4g", driver_max_result = "4g" ):
        pass

    @staticmethod
    def ui_url():
        def is_job_run():
            if "SNR_IS_JOB_RUN" in os.environ and os.environ["SNR_IS_JOB_RUN"] == 'True':
                return True   # Jupyter notebook or qtconsole
            else:
                return False  # Other type (?)

        port = 4040
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1',port))
            if result == 0:
                port += 1
                sock.close()
            else:
                port -= 1
                sock.close()
                break

        from IPython.core.display import display, HTML
        display(HTML(f"<a href='/proxy/{port}/jobs/' target='blank'>Open Spark UI</a>"))
        if is_job_run():
            try:
                import subprocess
                from jupyter_server import serverapp
                server_info = next(serverapp.list_running_servers())
                url = f"http://{server_info['hostname']}:{server_info['port']}/proxy/{port}/jobs/"
                formatted_url = f'print("Spark UI: \\033]8;;{url}\\033\\\\{url}\\033]8;;\\033\\\\")'
                subprocess.call(['python', '-c', formatted_url])
            except:
                pass

_=atexit.register(_SinaraSpark.stop_session)