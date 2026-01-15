import glob
import os

# Find all .py files in the current directory
python_files = glob.glob("*.py")
print(python_files)

# Define the root directory to start searching from
search_directory = "/home/usr/dir/" # Replace with your actual directory path

# Find all .txt files recursively
all_txt_files = glob.glob(os.path.join(search_directory, '**/*.txt'), recursive=True)

# Iterate and print the full paths of the found files
for file_path in all_txt_files:
    print(file_path)


# glob.glob(pathname, *, recursive=False): This is the most common function. It returns a list of path names that match the specified pattern. You can use standard Unix shell-style wildcards:
# *: Matches everything.
# ?: Matches any single character.
# [chars]: Matches any character in chars.
