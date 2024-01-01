import json

# Path to the full dataset file on your local system
full_dataset_path = r'C:\Users\Autre\Downloads\arxiv-metadata-oai-snapshot.json'  # Use a raw string
# Path to the partial dataset file that will be created
partial_dataset_path = 'C:\\Users\\Autre\\Desktop\\dataengineering\\dataset.json'  # Use double backslashes

# The target size for the partial dataset in bytes (200 KB)
target_size_in_bytes = 40 * 1024  # 200 KB

# Initialize a counter for the number of bytes written
bytes_written = 0

# Open the full dataset file and create the partial dataset file
with open(full_dataset_path, 'r', encoding='utf-8') as full_file, \
     open(partial_dataset_path, 'w', encoding='utf-8') as partial_file:
    
    # Write the beginning of a JSON array
    partial_file.write('[')
    
    # Read the first line (which should be '[' from the JSON array in the full dataset)
    full_file.readline()
    
    # Iterate over each line in the full dataset file
    for line in full_file:
        # Check if the next line would exceed the target size
        if bytes_written + len(line) > target_size_in_bytes:
            # Break the loop if the target size is reached or exceeded
            break
        
        # Write the line to the partial dataset file
        partial_file.write(line)
        
        # Update the bytes_written counter
        bytes_written += len(line)
        
        # If the line does not end with a comma, add one to separate JSON objects
        if not line.strip().endswith(','):
            partial_file.write(',')
    
    # Remove the last comma to maintain JSON format
    partial_file.seek(partial_file.tell() - 1)
    partial_file.truncate()
    
    # Write the end of the JSON array
    partial_file.write(']')

print(f'Partial dataset created at {partial_dataset_path}, size: {bytes_written} bytes')
