import pytest

from taigapy.utils import untangle_dataset_id_with_version


@pytest.mark.parametrize(
    "test_input,expected,expect_error",
    [
        ("foo.10/bar", ("foo", "10", "bar"), False),
        ("foo.10", ("foo", "10", None), False),
        ("foo-bar_baz.10", ("foo-bar_baz", "10", None), False),
        ("foo.bar", ("foo-bar_baz", "10", None), True),
    ],
)
def test_untangle_dataset_id_with_version(
    test_input: str, expected: str, expect_error: bool
):
    if not expect_error:
        assert untangle_dataset_id_with_version(test_input) == expected
    else:
        with pytest.raises(ValueError):
            untangle_dataset_id_with_version(test_input)
