import os
import zipfile

def zip_project(output_filename='project.zip'):
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('.'):
            for file in files:
                file_path = os.path.join(root, file)
                if file != output_filename:
                    arcname = os.path.relpath(file_path, '.')
                    zipf.write(file_path, arcname)

if __name__ == '__main__':
    zip_project()
    print("Project zipped successfully.")
