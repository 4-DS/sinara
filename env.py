

def get_cache_paths():
    cache_paths = {
        "test": "/home/jupyter/work/resources/data/cache/test",
        "prod": "/home/jupyter/work/resources/data/cache/prod",
        "user": "/home/jupyter/work/resources/data/cache/user"
    }
    return cache_paths

def get_cache_path(env_name):
    
    env_paths = get_cache_paths()
    if env_name not in env_paths:
        raise Exception("Unexpected env_name value:" + env_name)

    return env_paths[env_name]