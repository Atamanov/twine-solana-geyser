#!/bin/bash

# Wait for Grafana to be available
until $(curl --output /dev/null --silent --head --fail http://admin:admin@grafana:3000/api/health); do
    echo "Waiting for Grafana..."
    sleep 2
done

echo "Grafana is up. Creating library panels..."

# Create a folder for the library panels if it doesn't exist
# The folder UID is specified in the library panel JSON files
FOLDER_UID="twine-geyser"
FOLDER_TITLE="Twine Geyser"

# Check if folder exists
STATUS_CODE=$(curl --silent --output /dev/null --write-out "%{http_code}" http://admin:admin@grafana:3000/api/folders/${FOLDER_UID})

if [ "$STATUS_CODE" -eq 404 ]; then
  echo "Folder '${FOLDER_TITLE}' not found, creating it..."
  curl --silent --show-error -X POST -H "Content-Type: application/json" \
       -d "{\"uid\": \"${FOLDER_UID}\", \"title\": \"${FOLDER_TITLE}\"}" \
       http://admin:admin@grafana:3000/api/folders
  echo
else
  echo "Folder '${FOLDER_TITLE}' already exists."
fi


# Loop through each library panel JSON file and create it via the API
for file in /etc/grafana/provisioning/library_panels/*.json; do
    UID=$(basename "$file" .json)
    NAME=$(jq -r '.name' "$file")
    MODEL=$(jq '.model' "$file")

    echo "Processing panel: $NAME (uid: $UID)"

    # Check if a library panel with this UID already exists
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://admin:admin@grafana:3000/api/library-elements/${UID})

    if [ "$HTTP_CODE" -eq 404 ]; then
        echo "  -> Panel does not exist. Creating..."
        PAYLOAD=$(jq -n --arg name "$NAME" --arg uid "$UID" --arg folder_uid "$FOLDER_UID" --argjson model "$MODEL" '{ "folderUid": $folder_uid, "name": $name, "uid": $uid, "model": $model, "kind": 1 }')
        
        curl --silent --show-error -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://admin:admin@grafana:3000/api/library-elements
        echo
    else
        echo "  -> Panel already exists. Skipping creation."
    fi
done

echo "Library panel creation process finished."

# Define source and destination for the dashboard
DASHBOARD_SOURCE_FILE="/etc/grafana/dashboards_source/main-dashboard.json"
DASHBOARD_DEST_DIR="/var/lib/grafana/provisioned_dashboards"

# Create the destination directory
echo "Creating dashboard destination directory: ${DASHBOARD_DEST_DIR}"
mkdir -p "${DASHBOARD_DEST_DIR}"

# Copy the main dashboard JSON file to trigger provisioning
echo "Copying main dashboard to trigger provisioning..."
cp "${DASHBOARD_SOURCE_FILE}" "${DASHBOARD_DEST_DIR}/main-dashboard.json"

echo "Importer finished." 