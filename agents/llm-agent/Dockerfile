FROM python:3.11-slim
WORKDIR /app
COPY agents/llm-agent/requirements.txt .
RUN pip install -r requirements.txt
COPY agents/llm-agent/ /app/
CMD ["python", "main.py"]