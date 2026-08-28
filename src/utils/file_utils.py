import hashlib


def calculate_file_hash(path):
    # Create SHA-256 hasher | used in main.py to identify exact source file content
    hasher = hashlib.sha256()

    # Chunk loading so big files won't be loaded fully into memory
    with open(path, "rb") as file:
        while True:
            chunk = file.read(8192)
            if not chunk:
                break
            hasher.update(chunk)

    # Return as a hexadecimal string
    return hasher.hexdigest()
