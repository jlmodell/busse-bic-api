FROM python:3.8

COPY ./requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

EXPOSE 3637

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3637"]