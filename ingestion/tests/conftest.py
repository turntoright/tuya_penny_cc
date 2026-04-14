import pytest
import respx


@pytest.fixture
def mock_router():
    with respx.mock(base_url="https://openapi.test", assert_all_called=False) as router:
        yield router
