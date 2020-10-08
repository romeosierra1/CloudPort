using Amazon.S3;
using Amazon.S3.Model;
using CloudPortAPI.Config;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public class AwsBlobService : IBlobService
    {
        private AwsBlobStorageSettings _settings;
        private AWSSDKCredentials _cred;

        public AwsBlobService(AWSSDKCredentials cred, AwsBlobStorageSettings settings)
        {
            _settings = settings;
            _cred = cred;
        }

        public void CreateFolder(string folderName)
        {
            throw new NotImplementedException();
        }

        public void DeleteFile(string path)
        {
            throw new NotImplementedException();
        }

        public void DeleteFolder(string path)
        {
            throw new NotImplementedException();
        }

        public async Task DownloadFile(string folderName, string localFilePath, string fileName)
        {
            AmazonS3Client amazonS3Client = new AmazonS3Client(_cred.AwsAccessKeyId, _cred.AwsSecretAccessKey, Amazon.RegionEndpoint.APSoutheast1);

            var getRequest = new GetObjectRequest
            {
                BucketName = folderName,
                Key = fileName
            };

            using (FileStream downloadFileStream = File.OpenWrite(localFilePath))
            {
                using (GetObjectResponse response = await amazonS3Client.GetObjectAsync(getRequest))
                {
                    await response.ResponseStream.CopyToAsync(downloadFileStream);
                    downloadFileStream.Close();
                }
            }
        }

        public void ListFiles(string FolderName)
        {
            throw new NotImplementedException();
        }

        public async Task UploadFile(string folderName, string localFilePath, string fileName = "")
        {
            AmazonS3Client amazonS3Client = new AmazonS3Client(_cred.AwsAccessKeyId, _cred.AwsSecretAccessKey, Amazon.RegionEndpoint.APSoutheast1);

            var putRequest1 = new PutObjectRequest
            {
                BucketName = folderName,
                Key = fileName,
                FilePath = localFilePath,

            };

            PutObjectResponse response1 = await amazonS3Client.PutObjectAsync(putRequest1);
        }
    }
}
