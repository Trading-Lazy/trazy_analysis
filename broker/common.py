from trazy_analysis.models.order import Order


def get_rejected_order_error_message(order: Order) -> str:
    error_message = (
        f"{order.type.name} order (asset={order.asset.key()}, action={order.action.name}, "
        f"direction={order.direction.name}, "
        f"size={order.size}) could not be executed."
    )
    return error_message
