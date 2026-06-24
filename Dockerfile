FROM mcr.microsoft.com/azure-functions/python:4-python3.10

# Cài đặt Java JRE để chạy được PySpark
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureWebJobsFeatureFlags=EnableWorkerIndexing

# Sao chép file requirements.txt từ thư mục config trước để tận dụng cache
COPY config/requirements.txt /home/site/wwwroot/requirements.txt

# Cài đặt thư viện (nếu requirements.txt không đổi, bước này sẽ dùng CACHE)
RUN cd /home/site/wwwroot && \
    pip install -r requirements.txt

# Sau đó mới sao chép các file code còn lại
COPY . /home/site/wwwroot

# Sao chép các tệp tin cấu hình và mã nguồn ra ngoài root folder của container để Azure Functions hoạt động
RUN cp /home/site/wwwroot/config/host.json /home/site/wwwroot/host.json && \
    cp /home/site/wwwroot/config/local.settings.json /home/site/wwwroot/local.settings.json && \
    cp /home/site/wwwroot/src/index.html /home/site/wwwroot/index.html && \
    cp /home/site/wwwroot/src/etl_pipeline.py /home/site/wwwroot/function_app.py
