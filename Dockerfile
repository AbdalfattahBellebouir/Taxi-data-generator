FROM python:3.9.11-alpine3.15
# FROM python:3.9

WORKDIR /taxi
COPY . .
RUN apk --no-cache upgrade \
    && pip install --upgrade pip \
    && apk --no-cache add tzdata build-base gcc libc-dev g++ make git bash libsasl

RUN apk add --update --no-cache alpine-sdk bash python autoconf openssl-dev \
  && git clone -o ${LIBRDKAFKA_GIT_SHA1} https://github.com/edenhill/librdkafka.git /tmp/librdkafka \
  && cd /tmp/librdkafka/  \
  && ./configure \
  && make  \
  && make install

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "generate_trip.py"]