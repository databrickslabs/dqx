# Application for the DQX framework

This directory contains the web application for DQX, built with FastAPI (backend) and React (frontend).

## 📦 Development Tools

This app uses several tools for local development:

- **`uv`**: Python package manager and virtual environment tool
- **`apx`**: Databricks App eXtension framework - orchestrates development servers (backend, frontend, OpenAPI watcher)
  - ⚠️ **Development only dependency** - not used anywhere in production code
  - Installed from GitHub as a dev dependency
- **`bun`**: Fast JavaScript/TypeScript package manager and runtime
  - Used to install frontend dependencies and run the build process
  - Alternative to npm/yarn with better performance

> **📝 Important**: Lock files (`bun.lockb` and `uv.lock`) must be committed to git to ensure reproducible builds across environments. Do not add them to `.gitignore`.

## 🏗️ Architecture

- **Backend**: FastAPI application (`src/databricks_labs_dqx_app/backend/`)
  - REST API endpoints under `/api`
  - No Spark session in the app process — all Spark work is offloaded to a Databricks Job
  - Serves static frontend files using FastAPI's `StaticFiles` middleware
- **Frontend**: React + TypeScript (`src/databricks_labs_dqx_app/ui/`)
  - Built with Vite and TanStack Router
  - Compiled into `__dist__` directory during build
  - Static files (HTML, JS, CSS) are hosted by FastAPI at the root path `/`
- **Task Runner**: Serverless Databricks Job (`tasks/dqx_task_runner.py`)
  - Runs profiler and dry-run operations asynchronously
  - Uses Spark (PySpark) — the only place Spark is used in this project
  - Deployed as a workspace file via DABs alongside the app
- **Production**: Deployed as a Databricks App using `databricks bundle deploy`
  - Only the built artifacts are deployed, not the development tools
  - FastAPI serves both the API (`/api/*`) and the static UI (`/*`)

### Authentication Model

The app uses a **two-tier authentication model** — no admin-scoped REST calls are made by the app itself.

#### OBO (On-Behalf-Of) — user identity

Operations that must respect the logged-in user's permissions use the `X-Forwarded-Access-Token` header, injected automatically by Databricks when the app runs on the platform:

- **Unity Catalog browsing** (catalogs, schemas, tables, columns)
- **Temporary view creation** — the view inherits the user's table permissions so the job can only read data the user can access

```python
async def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """Return a WorkspaceClient for the logged-in user (OBO), cached for 45 min."""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    return await _create_obo_ws(token_hash, token)
```

#### SP (Service Principal) — app identity

Operations the app owns and manages run as the app's own service principal (no user token required):

- **Job submission** for profiler and dry-run tasks
- **Rules catalog CRUD** (reading and writing the rules Delta table)
- **Schema migrations** (creating and evolving Delta tables)
- **App settings** (reading and writing settings from the Delta table)

This ensures that:
- ✅ Users only see data they have permission to access
- ✅ No elevated privileges are required for browsing or profiling
- ✅ Internal app state (rules, settings) is managed consistently under the SP identity
- ✅ Audit logs correctly attribute actions to individual users

### Async Job Pattern (Profiler & Dry-Run)

Profiler and dry-run operations require Spark, which cannot run inside the app process. These operations follow an async job pattern:

```
User request
    │
    ├─ (OBO) Create temporary VIEW over the target table
    │         └─ View inherits user's table permissions
    │
    ├─ (SP) Submit Databricks Job with view_fqn + config
    │        └─ dqx_task_runner.py runs on serverless compute
    │              ├─ Reads from the temporary view
    │              ├─ Runs profiler / dry-run (PySpark)
    │              ├─ Writes results to Delta table
    │              └─ Drops the temporary view (finally block)
    │
    └─ Return run_id + job_run_id to the frontend
           └─ Frontend polls /status until complete
                  └─ Frontend fetches /results from Delta
```

The `DQX_JOB_ID` environment variable (injected from the `dqx-task-runner-job` bundle resource) identifies which job to submit runs to.

### Routing Structure

The application uses **two separate routing layers**:

#### Backend Routing (FastAPI)

Server-side routing in `app.py` defines two main routes:

1. **Main Route (`/`)** - Static Files (UI)
   - Serves the compiled React frontend from the `__dist__` directory
   - Uses FastAPI's `StaticFiles` middleware
   - **Catch-all**: Any request that doesn't match `/api/*` is treated as a UI route
   - Handles client-side routing (TanStack Router navigates without server requests)

2. **API Route (`/api`)** - Backend Endpoints
   - Defined in `router.py` using FastAPI's `APIRouter`
   - All backend endpoints follow the pattern: `/api/endpoint_name`
   - Each API endpoint is configured with:
     - **Response Model**: Pydantic model that defines the response structure (type-safe)
     - **operation_id**: Human-readable identifier for the endpoint (used for OpenAPI docs and client generation)
     - **Sync or Async Function**: Handler function that processes requests and returns data to the frontend

   Example endpoint structure:
   ```python
   @router.get("/version", response_model=VersionResponse, operation_id="getVersion")
   def get_version() -> VersionResponse:
       return VersionResponse(version=__version__)
   ```

#### Frontend Routing (TanStack Router)

Client-side routing in `ui/routes/` handles navigation within the React application:

- **Location**: Routes are defined in `ui/routes/` directory (file-based routing)
- **Purpose**: Enables Single Page Application (SPA) behavior
  - Navigation happens without full page reloads
  - Faster user experience - only data is fetched, not entire HTML pages
  - Browser back/forward buttons work correctly
  - Deep linking to specific pages (e.g., `/config`, `/runs/my-run`)
- **Type-Safe**: Route definitions are auto-generated in `ui/types/routeTree.gen.ts`
- **How It Works**:
  1. User clicks a link (e.g., "Profile" → `/profile`)
  2. TanStack Router intercepts the navigation
  3. React renders the new component without server request
  4. Only API calls to `/api/*` fetch data from backend

### Build Process

The build process (`uv run apx build`) performs three key operations:

1. **OpenAPI Schema Generation**
   - Extracts the API contract from FastAPI backend and saves it to `.apx/openapi.json`
   - Uses **orval** to auto-generate TypeScript types and React Query hooks in `ui/lib/api.ts`
   - This decouples frontend and backend — they communicate via the OpenAPI contract
   - Frontend doesn't need to import Python types directly; all types are generated from the spec

2. **UI Compilation**
   - Compiles the React/TypeScript UI from the `ui/` folder
   - Creates optimized production assets in the `__dist__/` directory
   - Bundles JavaScript, CSS, and static assets

3. **Python Wheel Creation**
   - Packages both backend Python code and compiled frontend (`__dist__/`) into a single wheel
   - Output location: `app/.build/databricks_labs_dqx_app-*-py3-none-any.whl`
   - Inspect package contents: `zipinfo .build/databricks_labs_dqx_app-*-py3-none-any.whl`

**Why we use wheel for distributing the app?**

- ✅ **Stable Packaging**: Industry-standard format for Python applications
- ✅ **Self-Contained Dependencies**: All metadata and dependencies are embedded in the wheel
- ✅ **No Manual requirements.txt**: The `requirements.txt` in `.build/` automatically points to the wheel
- ✅ **Production-Ready**: Simplified installation and deployment in production environments

## 🚀 Quick Start

### Prerequisites

Before you begin, ensure you have:

- **Python 3.11 or higher** installed
- **Node.js 18+** (for bun)
- **Databricks CLI** installed: `pip install databricks-cli`
- **Access to a Databricks workspace**
- **Enabled Preview in the workspace: Databricks Apps - On-Behalf-Of User Authorization**

### Development Mode

**1. Install bun** (if not already installed):

```bash
curl -fsSL https://bun.com/install | bash
bun --version
```

**2. Configure authentication** to Databricks workspace (choose one option):

**Option 1: Using Databricks CLI**
```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

**Option 2: Using a specific profile**

Create a `.env` file in the app directory:
```bash
DATABRICKS_CONFIG_PROFILE=<your-profile>
DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>
DQX_JOB_ID=<task-runner-job-id>   # optional for local dev; required for profiler/dry-run
```

This is useful when you have multiple [Databricks CLI profiles](https://docs.databricks.com/aws/en/dev-tools/cli/profiles) and want to use a specific one.

**3. Go into the app folder:**
```bash
cd app
```

**4. Create a Python virtual environment and install dependencies:**
```bash
uv sync
```

**5. Install JavaScript/TypeScript dependencies:**
```bash
uv run bun install
```

**6. Create a build** (the project requires compilation because it contains frontend):
```bash
uv run apx build
```

**7. Start all development servers** (backend, frontend, and OpenAPI watcher):
```bash
uv run apx dev start
```

This will start an apx development server, which in turn runs backend, frontend and OpenAPI watcher.
All servers run in the background, with logs kept in-memory of the apx dev server.

**8. Access the application:**
- **Frontend UI**: http://localhost:9001
- **Backend API**: http://localhost:9001/api
- **OpenAPI docs**: http://localhost:9001/docs (interactive API documentation)

### 💻 IDE Setup & AI-Assisted Development

**Running the Development Servers:**
- Start the servers in your IDE's integrated terminal (e.g., Cursor, VS Code) for the best experience
- Access the app at `http://localhost:9001` - you can open it in your browser or use your IDE's simple browser

**AI-Assisted Development Recommendations:**
- ✅ **Frontend (UI)**: Well-suited for AI code generation (e.g., Cursor Composer, Copilot)
  - Component structure and styling can be efficiently generated
  - React patterns and TypeScript types benefit from AI assistance
- ⚠️ **Backend (API)**: You can use AI code generation but a careful review is required
  - Business logic requires deep understanding and validation
  - Security and data handling need human oversight
  - AI can assist with boilerplate, but critical logic should be manually crafted

### 📊 Monitoring & Logs

```bash
# View all logs
uv run apx dev logs

# Stream logs in real-time for development
uv run apx dev logs -f

# Check server status
uv run apx dev status

# Stop all servers
uv run apx dev stop

# Upgrade apx
uv sync --upgrade-package apx
```

### 🔄 Development Workflow

**Adding a New API Endpoint:**

1. Define the endpoint in `backend/router.py` with response model and operation_id
2. Add Pydantic models in `backend/models.py` if needed
3. Run `uv run apx build` to regenerate the OpenAPI schema
4. The TypeScript types and React Query hooks are auto-generated in `ui/lib/api.ts`
5. Use the generated hooks in your React components

**Making UI Changes:**

1. Edit components in `ui/components/` or routes in `ui/routes/`
2. Hot reload automatically refreshes the browser (no manual refresh needed)
3. TypeScript types from the backend are available in `ui/lib/api.ts`
4. Use the generated React Query hooks for type-safe API calls

## ✅ Code Quality

Run type checking and linting for both TypeScript and Python:
```bash
uv run apx dev check
```

To lint and format the Python backend code, run the following command from the root directory of the DQX project:
```bash
make fmt
```

## 🧪 Testing

Run it from the root of the dqx project (not app folder):

**Run backend unit tests** (from project root):
```bash
make test
```

**Run integration tests** (from project root):
```bash
make integration
```

## 🚢 Deployment

> **Note**: Production deployment uses the **Databricks CLI**, not `apx`. The `apx` tool is only for local development.

Deploying a Databricks App requires **three steps**:

### Step 1: Deploy the Bundle

Deploy the app infrastructure using Databricks Asset Bundles (DAB):
```bash
cd app
databricks bundle deploy -p <your-profile>
```

This command:
- Builds the project using `uv run apx build` (compiles frontend, generates OpenAPI schema, creates wheel)
- Uploads the `.build/` and `tasks/` directories to workspace
- Creates/updates the following resources:
  - **App** (`databricks-labs-dqx-app`)
  - **SQL Warehouse** (`dqx-sql-warehouse`) — used for metadata queries
  - **Schema** (`<catalog>.dqx_app`) — Delta tables for rules, results, settings
  - **Job** (`dqx-app-task-runner`) — serverless job for profiler and dry-run tasks
- Configures app environment variables including `DQX_JOB_ID` (wired to the task runner job)

This does not:
- Start the app
- Deploy the actual source code to the app runtime
- Configure the OAuth scopes (you need to do this manually after the initial deployment)
- Grant users access to the app (you need to do this manually after deployment)

### Step 2: Configure OAuth Scopes (⚠️ Critical)

**Important**: After the initial deployment, you **must** enable additional OAuth scopes for the app to function properly. The default scopes configured in `databricks.yml` are not sufficient for all app features.

Run the following commands:

```bash
# 1. Login to your Databricks account (not workspace)
databricks auth login --host https://accounts.cloud.databricks.com --account-id <dbx-account-id> --profile <profile-name>

# 2. Update the OAuth app integration
databricks account custom-app-integration update '<oauth2-app-client-id>' --json '{"scopes": ["openid", "profile", "email", "all-apis", "offline_access", "iam.current-user"]}'
```

**Where to find the OAuth2 App Client ID:**
- **Option A - UI**: Navigate to Apps → Your App → User authorization section
- **Option B - CLI**: Run `databricks account custom-app-integration list`

**Why is this needed?**
The default scopes available in `databricks.yml` are limited. The `all-apis` scope grants the necessary permissions for the app to browse Unity Catalog and create temporary views on behalf of the user.

To confirm the scope has been added run:
```bash
databricks account custom-app-integration get '<oauth2-app-client-id>'
```

**Note**: This is a one-time configuration per app. You only need to do this after the initial deployment or when changing the app's OAuth integration.

### Step 3: Start the App Compute

**Using CLI:**
```bash
databricks apps start databricks-labs-dqx-app -p <your-profile>
```

**Using UI:**
1. Navigate to **Apps** in the sidebar
2. Find the **databricks-labs-dqx-app** app
3. Click **Start**

### Step 4: Deploy the Source Code

After the bundle is deployed, you need to deploy the actual source code to the app.

**Option A: Using Databricks CLI**
```bash
databricks apps deploy databricks-labs-dqx-app \
  --source-code-path /Workspace/Users/<your-username>/.bundle/databricks-labs-dqx-app/dev/files/.build \
  -p <your-profile>
```

**Option B: Using Databricks UI**
1. Navigate to **Apps** in the sidebar
2. Find and click on **databricks-labs-dqx-app**
3. Click the **Deploy** button
4. Enter the source code path: `/Workspace/Users/<your-username>/.bundle/databricks-labs-dqx-app/dev/files/.build`
5. Click **Deploy**

### Step 5: Configure Permissions in the app

After deployment, you need to grant users permissions to access the app.
This is done through the Databricks UI or API by assigning users/groups to the app with `Can Use` permission.

### Step 6: Access the App

Once the app is deployed and started, you can access it at:
```
https://<your-workspace-url>/apps/databricks-labs-dqx-app
```

**Finding the App URL:**
- **In Databricks UI**: Navigate to **Apps** in the sidebar → Click on **databricks-labs-dqx-app** → Copy the URL from the address bar
- **Via CLI**: Run `databricks apps get databricks-labs-dqx-app -p <your-profile>` and look for the `url` field

### Complete Deployment Script

Here's a complete script that performs all deployment steps:

```bash
cd app

# Step 1: Deploy bundle (builds wheel, provisions job + warehouse + schema)
databricks bundle deploy -p <your-profile>

# Step 2: Deploy source code
databricks apps deploy databricks-labs-dqx-app \
  --source-code-path /Workspace/Users/<your-username>/.bundle/databricks-labs-dqx-app/dev/files/.build \
  -p <your-profile>

# Step 3: Start the app
databricks apps start databricks-labs-dqx-app -p <your-profile>
```

After deployment, continue with [Step 5: Configure Permissions](#step-5-configure-permissions-in-the-app).

### Monitor and Manage

**Check app status:**
```bash
databricks apps get databricks-labs-dqx-app -p <your-profile>
```

**View app logs:**
```bash
databricks apps logs databricks-labs-dqx-app -p <your-profile>
```

**Stop the app:**
```bash
databricks apps stop databricks-labs-dqx-app -p <your-profile>
```

**Redeploy after code changes:**
```bash
# Deploy bundle
databricks bundle deploy -p <your-profile>

# Deploy updated source code
databricks apps deploy databricks-labs-dqx-app \
  /Workspace/Users/<your-username>/.bundle/databricks-labs-dqx-app/dev/files/.build \
  -p <your-profile>

# The app will automatically restart after deployment
```

**Update OAuth scopes (after changing `databricks.yml` or initial deployment):**

⚠️ **Important**: After the initial deployment, you must configure the `all-apis` scope. See [Step 2: Configure OAuth Scopes](#step-2-configure-oauth-scopes--critical) for details.

If you're updating scopes in `databricks.yml`:
```bash
# Deploy bundle with updated config
databricks bundle deploy -p <your-profile>

# Stop the app to clear old OAuth tokens
databricks apps stop databricks-labs-dqx-app -p <your-profile>

# Start the app to get new OAuth tokens with updated scopes
databricks apps start databricks-labs-dqx-app -p <your-profile>
```

## 🐛 Troubleshooting

### Bundle deployment errors

If you see errors like "App with name X does not exist or is deleted" during deployment:

```bash
# Clean local bundle cache
cd app
rm -rf .databricks

# Clean remote bundle state (optional)
databricks workspace delete /Workspace/Users/<your-username>/.bundle/<bundle-name> -p <your-profile> --recursive

# Try deploying again
databricks bundle deploy -p <your-profile>
```

Or use the `--force-deploy` flag to override existing state:
```bash
databricks bundle deploy -p <your-profile> --force
```

**When to clean the cache:**
- After changing the bundle or app name in `databricks.yml`
- When switching between different workspaces
- If deployment fails with "app does not exist" errors
- After manually deleting an app from the workspace UI

### Profiler or dry-run not starting

If submitting a profiler or dry-run returns an error about the job:

1. Verify `DQX_JOB_ID` is set in the app's environment (check `databricks.yml` and app config in the UI)
2. Confirm the `dqx-app-task-runner` job was created: `databricks jobs list -p <your-profile>`
3. Confirm the app's service principal has `CAN_MANAGE_RUN` on the job (set automatically by DABs)

### uv hangs

If any of the uv commands hang, try to diagnose:
```bash
# sync package in verbose mode
uv sync -v
```

A typical resolution is to remove the lock file:
```bash
rm -rf .venv/.lock
```

### Port already in use

If you see "port already in use" errors:
```bash
# Check what's using the port
lsof -i :9001

# Stop existing apx servers
uv run apx dev stop
```

### Missing static assets error

If the app fails to start with "Directory '__dist__' does not exist":
```bash
# Rebuild the frontend
uv run apx build
```

This is expected in CI/test environments - the app will skip serving static files if `__dist__` doesn't exist.

### OBO token issues in development

When running locally, the `X-Forwarded-Access-Token` header won't be present. Make sure:
- You've authenticated via `databricks auth login` or configured `.env`
- The backend is running on Databricks (for OBO to work) or use test overrides

### Build failures

If `apx build` fails:
```bash
# Clean build artifacts
rm -rf app/.build app/.apx app/__dist__

# Try building again
uv run apx build
```

### TypeScript type errors

If you see type errors in the UI:
```bash
# Regenerate API types from OpenAPI schema
uv run apx build
```

This updates `ui/lib/api.ts` with the latest backend types.
