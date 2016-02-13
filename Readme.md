# Upload files to AWS s3

Python script for uploading files to amazon S3 storage. Uploads files from file system or emails from database.

## Installation
Use pip to install dependencies

```
pip install -r requirements.txt
```

Create `config.yaml` from template `config-template.yaml` and fill the configuration variables

## Usage
```
python upload-images.py /path/to/your/directory/
```

```
python upload-emails.py /path/to/your/directory/
```