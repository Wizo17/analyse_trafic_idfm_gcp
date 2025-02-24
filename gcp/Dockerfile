FROM python:3.9-slim

WORKDIR /app

COPY cloud_run_functions_etl.sh /app/cloud_run_functions_etl.sh
COPY cloud_run_functions_etl.py /app/cloud_run_functions_etl.py
COPY requirements_cloud_run.txt /app/requirements_cloud_run.txt

RUN pip install --no-cache-dir -r requirements_cloud_run.txt

RUN apt-get update && apt-get install -y \
    git \
    unzip \
    zip \
    curl \
    gnupg \
    lsb-release

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update -y && apt-get install google-cloud-cli -y

RUN chmod +x /app/cloud_run_functions_etl.sh

EXPOSE 8080

CMD ["python", "cloud_run_functions_etl.py"]
