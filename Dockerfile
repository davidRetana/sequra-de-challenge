FROM python/3.9-slim AS base

WORKDIR /workspace

RUN apt update
RUN apt install -y curl

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml .
COPY etl .
RUN uv pip compile pyproject.toml > uv.lock

ENTRYPOINT ["python"]
CMD ["etl/extract_raw_data.py", "default_arg"]
