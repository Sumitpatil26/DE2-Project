from google.cloud import storage

# Initialize client using environment variable
client = storage.Client()

# List all buckets to verify authentication
buckets = list(client.list_buckets())

print("✅ Successfully authenticated!")
print("Buckets in your project:")
for bucket in buckets:
    print("-", bucket.name)