S3_JSON_BUCKET="s3://de-haguz-raw-useast1-dev/youtube/raw_statistics_reference_data/"
S3_CSV_BUCKET="s3://de-haguz-raw-useast1-dev/youtube/rawstatistics/"

# Download dataset from kaggle and set its path relative to the current folder from where the script is running
echo "Setting the dataset download path to current working directory..."
kaggle config set -n path -v ./
echo "Done!"

echo "Downloading the dataset from kaggle..."
kaggle d download datasnaek/youtube-new

# Path to the RAR file
OUTPUT_DIRECTORY="./datasets/datasnaek/youtube-new/"

# Dataset output path after extraction
RAR_FILE_DIRECTORY="${OUTPUT_DIRECTORY}youtube-new.zip"

echo "Starting files extraction..."

# Make sure to point to the correct path of unzip if it"s installed or install it if necessary
7z x "$RAR_FILE_DIRECTORY" -o"$OUTPUT_DIRECTORY"

echo "All matching files have been processed successfully."


# This will copy all JSON Reference data to same location:
aws s3 cp $OUTPUT_DIRECTORY $S3_JSON_BUCKET --recursive --exclude "*" --include "*.json"


# # To copy all data files to its own location, following Hive-style patterns:

# Base path for the destination
DESTINATION_BASE="{$S3_CSV_BUCKET}region="

# Directory where the video files are located
SOURCE_DIR="$OUTPUT_DIRECTORY"

# Find all CSV files in the source directory that match the pattern '*videos.csv'
find "$SOURCE_DIR" -name "*videos.csv" | while read file; do
    # Extract the file name from the path
    FILE_NAME=$(basename "$file")

    # Extract the region code from the filename by trimming 'videos.csv' and convert to lowercase
    REGION_CODE=$(echo "${FILE_NAME%videos.csv}" | tr '[:upper:]' '[:lower:]')

    # Construct the AWS S3 copy command
    COMMAND="aws s3 cp '$file' '${S3_CSV_BUCKET}${REGION_CODE}/'"

    # Execute the command
    echo "Executing: $COMMAND"
    eval $COMMAND
done

echo "All matching files have been copied successfully."
