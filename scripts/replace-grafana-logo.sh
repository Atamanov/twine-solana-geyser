#!/bin/bash

# Script to replace Grafana logos with custom logo
# Run this after Grafana container is started

GRAFANA_CONTAINER_NAME="grafana"
CUSTOM_LOGO_PATH="/config/logo_small.png"

echo "Replacing Grafana logos with custom logo..."

# Copy custom logo to various Grafana logo locations
docker exec $GRAFANA_CONTAINER_NAME sh -c "
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/grafana_icon.svg
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/grafana_com_auth_icon.svg
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/grafana_net_logo.svg
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/grafana_mask_icon.svg
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/fav32.png
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/fav16.png
    cp $CUSTOM_LOGO_PATH /usr/share/grafana/public/img/apple-touch-icon.png
"

echo "Grafana logos replaced. You may need to clear your browser cache to see the changes."