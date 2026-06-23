FROM mcr.microsoft.com/azure-functions/python:4-python3.10

# Cài đặt Java JRE để chạy được PySpark
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureWebJobsFeatureFlags=EnableWorkerIndexing

# Sao chép file requirements.txt trước để tận dụng cache
COPY requirements.txt /home/site/wwwroot/requirements.txt

# Cài đặt thư viện (nếu requirements.txt không đổi, bước này sẽ dùng CACHE)
RUN cd /home/site/wwwroot && \
    pip install -r requirements.txt

# Sau đó mới sao chép các file code còn lại
COPY . /home/site/wwwroot

# Bản sao etl_pipeline.py làm function_app.py trong container để Azure Functions nhận diện
RUN cp /home/site/wwwroot/etl_pipeline.py /home/site/wwwroot/function_app.py
