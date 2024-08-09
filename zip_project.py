import os
import zipfile
import fnmatch

def get_gitignore_patterns():
    if os.path.exists('.gitignore'):
        with open('.gitignore', 'r') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

def should_ignore(path, ignore_patterns):
    for pattern in ignore_patterns:
        if fnmatch.fnmatch(path, pattern):
            return True
    return False

def zip_project(output_filename='project.zip'):
    ignore_patterns = get_gitignore_patterns()
    
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('.'):
            for file in files:
                file_path = os.path.join(root, file)
                if not should_ignore(file_path, ignore_patterns) and file != output_filename:
                    arcname = os.path.relpath(file_path, '.')
                    zipf.write(file_path, arcname)

if __name__ == '__main__':
    zip_project()
    print("Project zipped successfully.")
