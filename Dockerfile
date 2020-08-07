FROM php:7.2-alpine
MAINTAINER maarten.schermer@naturalis.nl

RUN apk add --no-cache sqlite composer
RUN apk add jq
RUN docker-php-ext-install bcmath

RUN mkdir -p /code

RUN apk --update add git

ADD . /code
WORKDIR /code/validator
RUN composer install
COPY php.ini /usr/local/etc/php/
