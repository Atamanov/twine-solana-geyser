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
    echo "Processing $file..."
    
    # Extract name and uid from the file to use in the API payload
    # This assumes the filename matches the intended uid for the library panel
    BASENAME=$(basename "$file" .json)
    
    # Build the payload for the library element API
    # It requires a folderUid, name, model (the JSON content), and kind (1 for panel)
    PAYLOAD=$(jq -n --arg name "$BASENAME" --arg folder_uid "$FOLDER_UID" --argjson model "$(cat $file | jq '.model')" \
      '{folderUid: $folder_uid, name: $name, model: $model, kind: 1}')

    # POST the panel to Grafana
    curl --silent --show-error -X POST -H "Content-Type: application/json" \
         -d "$PAYLOAD" \
         http://admin:admin@grafana:3000/api/library-elements
    echo
done

echo "Library panel creation process finished."

# Reload dashboard provisioning to make sure dashboards pick up new panels
echo "Reloading dashboard provisioning..."
curl -X POST http://admin:admin@grafana:3000/api/admin/provisioning/dashboards/reload
echo

echo "Importer finished." 