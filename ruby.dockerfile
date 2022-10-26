FROM ruby:2.7

RUN true \
    && apt update \
    && apt install -y socat \
    && true

RUN true \
    # Removing loop deps error
    && gem install faraday -v 1.10.2 \
    && gem install redis \
    && true

COPY ./zz-export-pull-requests /gems/build/zz-export-pull-requests
COPY ./zz_bitbucket_rest_api /gems/build/zz_bitbucket_rest_api

RUN true \
    && cd /gems/build \
    && cd zz_bitbucket_rest_api \
    && gem build bitbucket_rest_api.gemspec \
    && gem install *.gem \
    && cd .. \
    && cd zz-export-pull-requests \
    && gem build export-pull-requests.gemspec \
    && gem install *.gem \
    && true
