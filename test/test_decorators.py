from common.decorators import try_until_success


def test_try_until_success():
    failures = 0
    MAX_FAILURES = 3

    @try_until_success
    def func_to_try():
        nonlocal failures
        if failures < MAX_FAILURES:
            failures += 1
            raise Exception

    func_to_try()

    assert failures == MAX_FAILURES
