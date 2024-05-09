
```bash
 git clone https://github.com/GoogleCloudPlatform/training-data-analyst
 cd /home/jupyter/training-data-analyst/quests/dataflow_python/
```

```bash
gcloud services enable dataflow.googleapis.com
```

```bash
# Create GCS buckets and BQ dataset
cd $BASE_DIR/../..
source create_batch_sinks.sh

# Generate event dataflow
source generate_batch_events.sh

# Change to the directory containing the practice version of the code
cd $BASE_DIR
```

