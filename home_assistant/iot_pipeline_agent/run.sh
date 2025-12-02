#!/usr/bin/with-contenv bashio

HA_TOKEN=$(bashio::config 'ha_token')
BACKEND_URL=$(bashio::config 'backend_url')
API_KEY=$(bashio::config 'api_key')

export HA_TOKEN BACKEND_URL API_KEY

exec python3 /usr/src/agent/main.py
