def get_profile_dict(profile_name, kwargs=None):
    return {
            'bentoservice_profile': {
                'name': profile_name,
                'params': kwargs
            }
        }

def SinaraOnnxBentoService(*args, **kwargs):
    
    def decorator(bento_service_cls):
        
        bento_service_cls.service_profile = get_profile_dict(SinaraOnnxBentoService.__name__, kwargs)
        return bento_service_cls
    
    return decorator

def SinaraPytorchBentoService(*args, **kwargs):

    def decorator(bento_service_cls):
        
        bento_service_cls.service_profile = get_profile_dict(SinaraPytorchBentoService.__name__, kwargs)
        return bento_service_cls
    
    return decorator

def SinaraBinaryBentoService(*args, **kwargs):

    def decorator(bento_service_cls):
        
        bento_service_cls.service_profile = get_profile_dict(SinaraBinaryBentoService.__name__, kwargs)
        return bento_service_cls
    
    return decorator