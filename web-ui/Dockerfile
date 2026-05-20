FROM python:3.11-slim
WORKDIR /app
COPY web-ui/requirements.txt .
RUN pip install -r requirements.txt
COPY web-ui/ /app/
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]