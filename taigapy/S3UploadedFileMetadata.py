class S3UploadedFileMetadata:
    def __init__(self, s3_object, filename, filetype):
        self.location = uploadS3Data["location"]
        self.eTag = uploadS3Data["ETag"]
        self.bucket = uploadS3Data["bucket"]
        self.key = uploadS3Data["key"]
        self.filename = filename
        self.filetype = str(filetype)
