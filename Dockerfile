# FROM python:3-alpine
FROM python:3.9

WORKDIR /taxi
COPY . .
RUN apt-get install apk git make
RUN apk add gcc libpq-dev && apk add musl-dev && apk add git && git clone https://github.com/edenhill/librdkafka.git && \
  cd librdkafka && git checkout tags/v1.9.0 && \
  ./configure && make && make install && \
  cd ../ && rm -rf librdkafka

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "generate_trip.py"]