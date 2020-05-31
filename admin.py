from django.contrib import admin

# Register your models here.

from .models import Candle
from .models import Action

admin.site.register([Action, Candle])
