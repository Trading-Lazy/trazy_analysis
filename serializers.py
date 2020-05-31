from rest_framework import serializers

from .models import Candle
from .models import Action

class CandleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Candle
        fields = [field.name for field in Candle._meta.get_fields()]

class ActionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Action
        fields = [field.name for field in Action._meta.get_fields()]
