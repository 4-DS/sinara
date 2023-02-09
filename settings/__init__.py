from ..common import importSinaraModuleClass
from .secure_connection import check_secure_connection

check_secure_connection()

_SinaraSettings = importSinaraModuleClass(module_name = "settings", class_name = "_SinaraSettings")