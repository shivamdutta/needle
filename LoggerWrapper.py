import logging

class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called. """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Singleton(_Singleton('SingletonMeta', (object,), {})): pass

class Logger(Singleton):

    def __init__(self, log_filename, log_level):

        logging.basicConfig(filename=log_filename, filemode='a', format='%(process)d - %(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
        root = logging.getLogger()
        
        if log_level=='DEBUG':
            root.setLevel(logging.DEBUG)
        elif log_level=='INFO':
            root.setLevel(logging.INFO)
        elif log_level=='WARNING':
            root.setLevel(logging.WARNING)
        elif log_level=='CRITICAL':
            root.setLevel(logging.CRITICAL)
        elif log_level=='ERROR':
            root.setLevel(logging.ERROR)
        
        self.logging = logging