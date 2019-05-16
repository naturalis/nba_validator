FROM php:7.2-alpine
MAINTAINER maarten.schermer@naturalis.nl

RUN apk add --no-cache sqlite composer
RUN apk add jq
RUN docker-php-ext-install bcmath

RUN mkdir -p /code
RUN mkdir -p /config
RUN mkdir -p /schemas

RUN apk --update add git

ADD . /code
WORKDIR /code/validator
RUN composer install
COPY php.ini /usr/local/etc/php/

ARG CACHEBUST=1

WORKDIR /config
RUN git clone https://github.com/naturalis/nba_validator_config

WORKDIR /schemas
RUN git clone https://github.com/naturalis/nba_json_schemas
