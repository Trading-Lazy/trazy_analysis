from django.shortcuts import get_object_or_404
from .models import Candle
from .models import Action
from rest_framework import viewsets
from rest_framework.response import Response
from .serializers import CandleSerializer
from .serializers import ActionSerializer

class CandleViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows candles to be viewed or edited.
    """
    queryset = Candle.objects.all().order_by('-timestamp')
    serializer_class = CandleSerializer

class ActionViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows actions to be viewed or edited.
    """
    queryset = Action.objects.all().order_by('timestamp')
    serializer_class = ActionSerializer

    def list(self, request):
        queryset = Action.objects.all().order_by('-timestamp')
        serializer = ActionSerializer(queryset, many=True)
        response = Response(serializer.data)
        response['Access-Control-Allow-Origin'] = '*'
        return response

    def retrieve(self, request, pk=None):
        queryset = Action.objects.all()
        action = get_object_or_404(queryset, pk=pk)
        serializer = ActionSerializer(action)
        response = Response(serializer.data)
        response['Access-Control-Allow-Origin'] = '*'
        return response
