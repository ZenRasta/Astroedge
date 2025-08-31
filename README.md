# AstroEdge

Astrological edge detection for prediction markets. Identifies potential mispricing in Polymarket using astrological aspects as a weak signal layer.

## Quick Start

### 1. Supabase Setup

1. Create a new Supabase project at [supabase.com](https://supabase.com)
2. Go to Settings → API to get your credentials:
   - `SUPABASE_URL`: Your project URL
   - `SUPABASE_SERVICE_ROLE`: Service role key (secret)  
   - `SUPABASE_ANON`: Anonymous public key

### 2. Environment Configuration

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your Supabase credentials
vim .env
```

### 3. Development with Docker

```bash
# Start all services
docker compose -f docker-compose.dev.yml up --build

# Backend will be available at http://localhost:8000
# Health check: http://localhost:8000/health
# API docs: http://localhost:8000/docs
```

### 4. Development without Docker

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run the backend directly
cd backend
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Project Structure

```
astroedge/
├── backend/                 # FastAPI application
│   ├── config.py           # Pydantic settings
│   ├── main.py            # FastAPI app with /health, /version
│   ├── supabase_client.py # Supabase wrapper
│   ├── routers/           # API route handlers
│   └── services/          # Business logic
├── bot/                   # Telegram bot (aiogram)
├── webapp/               # Frontend (Telegram mini-app)
├── sql/                  # Database schemas
├── .github/workflows/    # CI/CD
└── docker-compose.dev.yml
```

## API Endpoints

- `GET /health` - Health check with Supabase connectivity test
- `GET /version` - Application version info
- `GET /docs` - Interactive API documentation

## Configuration

Key environment variables (see `.env.example`):

**Supabase:**
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE`, `SUPABASE_ANON`

**Trading Parameters:**
- `FEE_BPS_DEFAULT=60` - Fee basis points
- `SPREAD_DEFAULT=0.01` - Default spread
- `SLIPPAGE_DEFAULT=0.005` - Slippage estimate

**Astro Scoring:**
- `LAMBDA_GAIN=0.10` - Astro signal strength
- `EDGE_THRESHOLD=0.04` - Minimum edge to trade
- `LAMBDA_DAYS=5` - Temporal decay parameter
- `K_CAP=5.0` - Maximum astro score cap

## Development

### Code Quality

```bash
# Format code
black backend/

# Lint
ruff check backend/

# Type checking
mypy backend/

# Run tests
pytest backend/
```

### Database

Database schemas will be in the `sql/` directory. Apply them to your Supabase project through the Supabase dashboard or CLI.

## Deployment

The CI pipeline runs on every push and PR:
- Linting (Ruff)
- Formatting (Black)  
- Type checking (MyPy)
- Tests (Pytest)
- Docker build validation

## Next Steps

1. **Complete Supabase setup** - Create your project and update `.env`
2. **Database schema** - Apply the astro scoring tables in `sql/`
3. **Market integration** - Connect to Polymarket API
4. **Aspect computation** - Implement the astrological calculations

## Architecture

AstroEdge uses a layered approach:
1. **Aspect Events** - Quarterly astrological calendar
2. **Market Scanning** - Polymarket integration  
3. **Category Mapping** - Aspects → market categories
4. **Edge Detection** - Probability adjustment + mispricing
5. **Position Sizing** - Conservative Kelly with caps

The system is designed to provide a **weak, transparent signal** that nudges market probabilities rather than being the primary trading basis.