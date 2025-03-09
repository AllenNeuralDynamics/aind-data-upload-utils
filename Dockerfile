FROM python:3.10-slim
WORKDIR /app
ADD src ./src
ADD pyproject.toml .
ADD setup.py .

# Add git in case we need to install from branches
RUN apt-get update && apt-get install -y git

# Pip command. Without '-e' flag, index.html isn't found. There's probably a
# better way to add the static html files to the site-packages.
RUN pip install . --no-cache-dir
