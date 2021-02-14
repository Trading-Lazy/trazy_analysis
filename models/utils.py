from models.enums import Action, Direction


def is_closed_position(action: Action, direction: Direction):
    return (
        action == Action.SELL
        and direction == Direction.LONG
        or action == Action.BUY
        and direction == direction.SHORT
    )


def is_open_position(action: Action, direction: Direction):
    return not is_closed_position(action, direction)
