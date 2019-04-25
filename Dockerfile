FROM php:7.2-alpine
MAINTAINER maarten.schermer@naturalis.nl

RUN apk add --no-cache sqlite composer
RUN docker-php-ext-install bcmath
RUN mkdir -p /code
ADD . /code
WORKDIR /code/tools/validator/PHP
RUN composer install
COPY php.ini /usr/local/etc/php/
RUN apk add jq
