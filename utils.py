import os

def split_file_path(filepath):
    base_dir, filename = os.path.split(filepath)
    file_name_without_extension, file_extension = os.path.splitext(filename)
    return base_dir, file_name_without_extension, file_extension