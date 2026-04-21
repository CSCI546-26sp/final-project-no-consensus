import os
import pandas as pd
import re
import random
import string

def sanitize_filename(title):
    """Sanitizes a string to be used as a filename."""
    # Remove invalid characters
    s = re.sub(r'[<>:"/\\|?*]', '', title)
    # Replace spaces with underscores, or just keep them based on preference
    s = s.replace(' ', '_')
    # Trim leading/trailing whitespace and ensure it's not empty
    s = s.strip()
    if not s:
        s = "untitled_text"
    return s[:200] # Limit filename length

def generate_random_filename(length=10):
    """Generates a random string to be used as a filename."""
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length)) + '.txt'

directory_path = '/Users/rajivmurali/Downloads/archive/'
output_directory = './data/wiki/biggest-text-files/'

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# New parameters for target file size and count
target_file_size_mb = 1
target_file_size_bytes = target_file_size_mb * 1024 * 1024 * 1024
max_output_files = 4

# Initialize accumulators for the current output file
current_file_content_buffer = [] # Stores lines for the current output file
current_file_buffer_size_bytes = 0
output_file_count = 0

stop_processing_all = False # Flag to stop all processing loops

parquet_files = [f for f in os.listdir(directory_path) if f.endswith('.parquet')][5:]

# Iterate over each parquet file
for file_name in parquet_files:
    if stop_processing_all:
        break

    file_path = os.path.join(directory_path, file_name)
    print(f"Processing {file_path}...")
    try:
        df_temp = pd.read_parquet(file_path)

        # Iterate over each row in the DataFrame
        for index, row in df_temp.iterrows():
            if stop_processing_all:
                break

            # Extract title and text, providing a generic name if title not found
            title = row.get('title', f'entry_{index}')
            text = row.get('text', '')

            # Only process non-empty string texts
            if isinstance(text, str) and text.strip():
                words = text.split()
                # Group words into chunks of 20 per line
                lines = [' '.join(words[i:i+20]) for i in range(0, len(words), 20)]
                formatted_text_block = '\n'.join(lines) + '\n' # Add a newline to separate content blocks

                # Get the size of the current text block in bytes (UTF-8 encoding)
                block_size_bytes = len(formatted_text_block.encode('utf-8'))

                # Check if adding this block would make the current file exceed target_file_size_bytes
                if current_file_buffer_size_bytes + block_size_bytes > target_file_size_bytes:
                    # If there's content in the buffer, write it to a new file
                    if current_file_content_buffer: # Ensure buffer is not empty
                        output_filename = os.path.join(output_directory, generate_random_filename())
                        with open(output_filename, 'w', encoding='utf-8') as f:
                            f.write(''.join(current_file_content_buffer))
                        output_file_count += 1
                        print(f"  Saved file {output_file_count}/{max_output_files} to '{output_filename}' (Size: {current_file_buffer_size_bytes / (1024*1024):.2f} MB)")

                        # Check if we reached the maximum number of output files
                        if output_file_count >= max_output_files:
                            print(f"Reached maximum of {max_output_files} output files. Stopping processing.")
                            stop_processing_all = True
                            break

                    # Start a new buffer with the current block
                    current_file_content_buffer = [formatted_text_block]
                    current_file_buffer_size_bytes = block_size_bytes
                else:
                    # Add the block to the current buffer
                    current_file_content_buffer.append(formatted_text_block)
                    current_file_buffer_size_bytes += block_size_bytes

        # Delete the parquet file after processing all its rows
        # Only delete if we are not stopping midway due to max file count and if it was successfully processed
        if not stop_processing_all:
            # os.remove(file_path)
            print(f"Successfully processed and deleted {file_name}.")

    except Exception as e:
        print(f"Error processing {file_name}: {e}")

# After all parquet files are iterated, write any remaining content in the buffer
if current_file_content_buffer and not stop_processing_all:
    output_filename = os.path.join(output_directory, generate_random_filename())
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(''.join(current_file_content_buffer))
    output_file_count += 1
    print(f"  Saved final file {output_file_count}/{max_output_files} to '{output_filename}' (Size: {current_file_buffer_size_bytes / (1024*1024):.2f} MB)")

print("Processing complete.")