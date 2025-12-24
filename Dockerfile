FROM python:3.12-slim

# 使用国内 apt 源（阿里云）
RUN set -eux; \
    echo "deb http://mirrors.aliyun.com/debian bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list; \
    echo "deb http://mirrors.aliyun.com/debian bookworm-updates main contrib non-free non-free-firmware" >> /etc/apt/sources.list; \
    echo "deb http://mirrors.aliyun.com/debian-security bookworm-security main contrib non-free non-free-firmware" >> /etc/apt/sources.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends build-essential zlib1g-dev libjpeg62-turbo-dev libpng-dev; \
    rm -rf /var/lib/apt/lists/*

# 使用国内 pip 源（清华）
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 先复制依赖文件，加快缓存利用
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

# 再复制项目源码
COPY . .

# 项目运行在 5003 端口
EXPOSE 5003

# 使用 app.py 作为入口
CMD ["python", "app.py"]
