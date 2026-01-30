# Application for the DQX framework

## 🚀 Quick Start

### Development Mode

Install `bun` (if not already installed):

```bash
curl -fsSL https://bun.com/install | bash
bun --version
```

To enable authentication, configure a [Databricks CLI profile](https://docs.databricks.com/aws/en/dev-tools/cli/profiles) for the app.
The recommended approach is to create a `.env` file in the app directory with the following content:
```bash
DATABRICKS_CONFIG_PROFILE=<your-profile>
```

Install JavaScript/TypeScript dependencies:
```bash
uv run bun install
```

Create a build:

```bash
uv run apx build
```

Start all development servers (backend, frontend, and OpenAPI watcher) in detached mode:
```bash
# go to the app folder and run:
uv run apx dev start
```

This will start an apx development server, which in it's turn runs backend, frontend and OpenAPI watcher.
All servers run in the background, with logs kept in-memory of the apx dev server.

### 📊 Monitoring & Logs

```bash
# View all logs
uv run apx dev logs

# Stream logs in real-time
uv run apx dev logs -f

# Check server status
uv run apx dev status

# Stop all servers
uv run apx dev stop
```

## ✅ Code Quality

Run type checking and linting for both TypeScript and Python:
```bash
uv run apx dev check
```

To lint and format the Python backend code, run the following command from the root directory of the DQX project:
```bash
make fmt
```

## 🚢 Deployment

Deploy to Databricks:

```bash
databricks bundle deploy -p <your-profile>
```

## Common Issues

### uv hangs

If any of the uv commands hang, try to run to diagnose:
```bash
# sync package in verbose mode
uv sync -v
```

A typical resolution is to remove the lock file
```bash
rm -rf .venv/.lock
```