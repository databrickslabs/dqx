# Application for the DQX framework

## 🚀 Quick Start

### Development Mode

Start all development servers (backend, frontend, and OpenAPI watcher) in detached mode:

```bash
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

## 📦 Build

Create a production-ready build:

```bash
uv run apx build
```

## 🚢 Deployment

Deploy to Databricks:

```bash
databricks bundle deploy -p <your-profile>
```
