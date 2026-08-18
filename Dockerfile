# Inkbox tunnels require POSIX; connect() raises on Windows. That is the whole
# reason this is a container rather than a script on the host.
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
      "inkbox>=0.1" \
      "fastapi>=0.115" \
      "uvicorn>=0.32" \
      "psycopg[binary]>=3.2"

COPY app.py bootstrap.py ./

# Runs as a non-root user. The process needs outbound HTTPS and outbound
# Postgres and nothing else, so it has no reason to hold root.
RUN useradd --create-home --uid 10001 gateway
USER gateway

# No EXPOSE and no published ports on purpose. Inbound traffic arrives down the
# tunnel's outbound connection, not through a listening socket. Publishing 8080
# would put the receiver on your LAN for no benefit.
CMD ["python", "-u", "app.py"]
