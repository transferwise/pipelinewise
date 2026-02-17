import pytest
from tap_mixpanel.client import MixpanelClient


@pytest.fixture
def mixpanel_client():
    mixpanel_client = MixpanelClient('API_SECRET', 'mixpanel.com', 300)
    mixpanel_client._MixpanelClient__verified = True
    return mixpanel_client
