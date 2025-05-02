FROM python:3.10-slim-buster

# Set environment variables
ENV PIP_DISABLE_PIP_VERSION_CHECK 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Use root user to install additional packages
USER root

# Install dependencies for Microsoft ODBC Driver
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Download and configure the Microsoft repository
RUN curl -sSL -O https://packages.microsoft.com/config/debian/10/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb

# Install Microsoft ODBC Driver 17 for SQL Server and tools
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools && \
    apt-get install -y --reinstall build-essential && \
    rm -rf /var/lib/apt/lists/*

# Add mssql-tools to PATH
ENV PATH="$PATH:/opt/mssql-tools/bin"

# Populate "odbcinst.ini" for Microsoft ODBC Driver
RUN echo "[ODBC Driver 17 for SQL Server]\n\
    Description=Microsoft ODBC Driver 17 for SQL Server\n\
    Driver=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1\n\
    Setup=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1" >> /etc/odbcinst.ini

WORKDIR /usr/src/app/

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . /usr/src/app/

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5000"]
