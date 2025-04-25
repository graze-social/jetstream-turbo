# Use a lightweight Python image
FROM python:3.11-slim
# Can pass a lockfile as a build arg
ARG LOCKFILE=pdm.lock
# Set environment variables
# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install PDM
RUN pip install pdm

ENV PDM_VENV_IN_PROJECT=1
ENV PATH="/app/.venv/bin:$PATH"

# Copy only dependency files first (better caching)
# Casey's Note: README.md and LICENSE are required for installing self as package
COPY README.md LICENSE pyproject.toml ${LOCKFILE} ./

# Install dependencies properly inside the virtual environment
RUN pdm install \
    --prod \
    --fail-fast \
    --lockfile ${LOCKFILE}


# Copy the rest of the project files (excluding `.venv`)
COPY src ./src/

CMD ["pdm", "run", "turbocharger"]
