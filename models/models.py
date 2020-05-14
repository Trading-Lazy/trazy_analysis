from django.utils import timezone
from djongo import models
from django.db.models import UniqueConstraint
from jsonfield import JSONField

STRING_MAX_LENGTH = 200


class Candle(models.Model):
    symbol = models.CharField(max_length=STRING_MAX_LENGTH, default='DUMMY_SYMBOL')

    PRICE_DECIMAL_PLACES = 5
    PRICE_INTEGER_PLACES = 8
    PRICE_MAX_DIGITS = PRICE_DECIMAL_PLACES + PRICE_INTEGER_PLACES
    open = models.DecimalField(
        max_digits=PRICE_MAX_DIGITS,
        decimal_places=PRICE_DECIMAL_PLACES
    )

    high = models.DecimalField(
        max_digits=PRICE_MAX_DIGITS,
        decimal_places=PRICE_DECIMAL_PLACES
    )

    low = models.DecimalField(
        max_digits=PRICE_MAX_DIGITS,
        decimal_places=PRICE_DECIMAL_PLACES
    )

    close = models.DecimalField(
        max_digits=PRICE_MAX_DIGITS,
        decimal_places=PRICE_DECIMAL_PLACES
    )

    volume = models.IntegerField()

    timestamp = models.DateTimeField('Time of the candle', default=timezone.now)

    class Meta:
        ordering = ['timestamp']
        constraints = [
            UniqueConstraint(fields=['timestamp', 'symbol'], name='candles_unique_identifier')
        ]


def make_enum(choices, default):
    max_length = 0
    for choice in choices:
        value = choice[0]
        max_length = max(max_length, len(value))
    return models.CharField(
        max_length=max_length,
        choices=choices,
        default=default
    )


class Action(models.Model):
    # action type
    BUY = 'BUY'
    SELL = 'SELL'
    ACTION_TYPE_CHOICES = [
        (BUY, 'Buy'),
        (SELL, 'Sell')
    ]
    action_type = make_enum(ACTION_TYPE_CHOICES, BUY)

    # position type
    LONG = 'LONG'
    SHORT = 'SHORT'
    POSITION_TYPE_CHOICES = [
        (LONG, 'Long'),
        (SHORT, 'Short')
    ]
    position_type = make_enum(POSITION_TYPE_CHOICES, LONG)

    CONFIDENCE_LEVEL_DECIMAL_PLACES = 3
    CONFIDENCE_LEVEL_INTEGER_PLACES = 1
    CONFIDENCE_LEVEL_MAX_DIGITS = CONFIDENCE_LEVEL_DECIMAL_PLACES + CONFIDENCE_LEVEL_INTEGER_PLACES
    confidence_level = models.DecimalField(
        max_digits=CONFIDENCE_LEVEL_MAX_DIGITS,
        decimal_places=CONFIDENCE_LEVEL_DECIMAL_PLACES
    )

    strategy = models.CharField(max_length=STRING_MAX_LENGTH)

    symbol = models.CharField(max_length=STRING_MAX_LENGTH)

    candle_id = models.BigIntegerField(null=True)

    parameters = JSONField()

    timestamp = models.DateTimeField('Time at which the action needs to be taken', default=timezone.now)

    # Get a list of actions from the database in descending order
    class Meta:
        ordering = ['-timestamp']
        constraints = [
            UniqueConstraint(fields=['timestamp', 'symbol', 'strategy'], name='actions_unique_identifier')
        ]
        indexes = [
            models.Index(fields=['parameters'], name='parameters_index')
        ]
