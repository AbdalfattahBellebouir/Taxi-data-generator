FROM python:3.9.11-alpine3.15
# FROM python:3.9

WORKDIR /taxi
COPY . .
RUN apk --no-cache upgrade \
    && pip install --upgrade pip \
    && apk --no-cache add tzdata build-base gcc libc-dev g++ make git bash libsasl

RUN git clone https://github.com/edenhill/librdkafka.git && cd librdkafka \
    && git checkout tags/v2.2.0 && ./configure --clean \
    && ./configure --prefix /usr/local \
    && make && make install

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "generate_trip.py"]