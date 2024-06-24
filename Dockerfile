FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

LABEL authors="t.iakhshigulov@innopolis.university"

# Installing make
RUN apt-get update

# Installing poetry and project dependencies
COPY poetry.lock .
COPY pyproject.toml .

RUN python -m pip install --no-cache-dir poetry && \
    poetry install --no-interaction --no-ansi

# Copying necessary project sources
COPY main.py main.py

CMD ["poetry", "run", "python", "main.py"]