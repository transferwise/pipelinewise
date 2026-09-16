from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_main_image_configures_mbuffer_status_interval():
    """The main image reduces mbuffer status noise without changing its command."""
    config = (REPOSITORY_ROOT / 'mbuffer.rc').read_text(encoding='utf-8')
    dockerfile_lines = (REPOSITORY_ROOT / 'Dockerfile').read_text(encoding='utf-8').splitlines()

    assert config == 'StatusInterval = 30\n'
    assert dockerfile_lines.count('COPY mbuffer.rc /etc/mbuffer.rc') == 1
