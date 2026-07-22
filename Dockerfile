# =============================================================================
# Dev image for the Laravel app that sits in front of the Debezium CDC pipeline.
# Code is bind-mounted (see docker-compose.yml), so this image only provides the
# PHP runtime + extensions. Vendor deps come from the mounted host ./vendor.
# =============================================================================
FROM php:8.3-cli

# System libraries needed to build the PHP extensions below.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      unzip \
      libzip-dev \
      libicu-dev \
      libonig-dev \
      librdkafka-dev \
      default-mysql-client \
    && docker-php-ext-install pdo_mysql bcmath zip intl \
    && pecl install redis rdkafka \
    && docker-php-ext-enable redis rdkafka \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Composer, for the occasional in-container `composer` run.
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html

EXPOSE 8000

CMD ["php", "artisan", "serve", "--host=0.0.0.0", "--port=8000"]
