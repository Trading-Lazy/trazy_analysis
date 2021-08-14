from abc import ABCMeta

from ratelimit import limits, sleep_and_retry


class InheritableSingletonMeta(ABCMeta):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class RateLimitedSingletonMeta(InheritableSingletonMeta):
    instances = {}

    def __init__(cls, cls_name, bases, namespace, **kwds):
        if not isinstance(cls.MAX_CALLS, property) and not isinstance(
            cls.PERIOD, property
        ):
            from trazy_analysis.common.helper import request

            setattr(
                cls,
                "request",
                sleep_and_retry(
                    limits(calls=cls.MAX_CALLS, period=cls.PERIOD)(request)
                ),
            )
        super().__init__(cls_name, bases, namespace, **kwds)
