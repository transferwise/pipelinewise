# Local Development Environment

This folder contains Docker-based development environment for target-iceberg.

## Prerequisites

- Docker
- Docker Compose

## Quick Start
1. **Start the development container:**
    ```bash
    cd local-dev
    docker compose up -d
    ```

2. **Access the container shell:**
    ```bash
    docker exec -it target-iceberg-dev bash
    ```
3. **Inside the container

    Refer to the [Makefile](../Makefile) for testing, linting, and other commands.

## Container Details

- **Platform:** linux/amd64
- **Python Version:** 3.10
- **Working Directory:** `/workspace` (mounted from project root)
- **Auto-installed:** All dependencies from `setup.py[test]`

## Common Commands

### Stop the container
```bash
docker compose down
```

### Rebuild the container (after dependency changes)
```bash
docker compose down
docker compose up -d --build
```

### View logs
```bash
docker compose logs -f
```

### Run a one-off command
```bash
docker compose run --rm target-iceberg-dev pytest tests/unit
```

## Troubleshooting

### Container won't start
Check logs:
```bash
docker compose logs
```

### Dependency issues
Rebuild the container:
```bash
docker compose down
docker compose up -d --build
```

### Changes not reflected
The project directory is mounted as a volume, so code changes should be immediately available. If not, try restarting:
```bash
docker compose restart
```
