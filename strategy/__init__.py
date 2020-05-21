import os

from django.conf import settings
import django
from django.core.exceptions import ImproperlyConfigured


def get_env_variable(name):
    """Gets the environment variable or throws ImproperlyConfigured
       exception
       :rtype: object
    """
    try:
        return os.environ[name]
    except KeyError:
        raise ImproperlyConfigured(
                    'Environment variable “%s” not found.' % name)

if not settings.configured:
    settings.configure(
    DATABASES={
        'default': {
            'ENGINE': get_env_variable('DATABASE_ENGINE'),
            'ENFORCE_SCHEMA': True,
            'NAME': get_env_variable('DATABASE_CONNECTION_NAME'),
            'CLIENT': {
                'host': get_env_variable('DATABASE_URL'),
                'username': get_env_variable('DATABASE_USERNAME'),
                'password': get_env_variable('DATABASE_PASSWORD'),
                'authMechanism': get_env_variable('DATABASE_AUTH_MECANISM')
            }
        }
    },
    INSTALLED_APPS = [
        'actionsapi.apps.ActionsApiConfig',
    ])
    django.setup()