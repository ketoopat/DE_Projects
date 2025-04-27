FROM python:3.10-slim

WORKDIR /streamlit_app

COPY requirements.txt .
RUN pip install -r requirements.txt

# DO NOT COPY app.py here
# Let volume mount handle it dynamically

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
