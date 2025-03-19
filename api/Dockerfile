FROM python:3.12.7
RUN apt-get update
RUN apt-get install -y iputils-ping
ADD . /api
WORKDIR /api
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
EXPOSE 8000
CMD ["gunicorn", "-w 3", "-b", "0.0.0.0:8000", "scsw_servers:app"]
