using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public class GoogleBlobService : IBlobService
    {
        private string _projectId;
        private StorageClient client;
        public GoogleBlobService(string projectId, string jsonPath)
        {
            _projectId = projectId;
            var credential = GoogleCredential.FromFile(jsonPath);
            client = StorageClient.Create(credential);
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
            // Create a bucket with a globally unique name
            var bucketName = folderName;
            
            // Download file
            using (var stream = File.OpenWrite(localFilePath))
            {
                await client.DownloadObjectAsync(bucketName, fileName, stream);
            }
        }

        public void ListFiles(string FolderName)
        {
            throw new NotImplementedException();
        }

        public async Task UploadFile(string folderName, string localFilePath, string fileName = "")
        {
            var bucketName = folderName;
            //client.CreateBucket(_projectId, bucketName);

            // Upload some files
            await client.UploadObjectAsync(bucketName, fileName, "*/*", File.OpenRead(localFilePath));
        }
    }
}
