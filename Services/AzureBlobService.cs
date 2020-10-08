using Azure.Storage.Blobs;
using CloudPortAPI.Config;
using System;
using System.IO;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public class AzureBlobService : IBlobService
    {
        BlobServiceClient _blobServiceClient;
        public AzureBlobService(AzureBlobStorageSettings azureBlobStorageSettings)
        {
           _blobServiceClient = new BlobServiceClient(azureBlobStorageSettings.ConnectionString);
        }
        public void CreateFolder(string folderName)
        {
            _blobServiceClient.CreateBlobContainer(folderName);
        }

        public void DeleteFile(string path)
        {
            throw new NotImplementedException();
        }

        public void DeleteFolder(string path)
        {
            throw new NotImplementedException();
        }

        public async Task DownloadFile(string folderName, string localFilePath ,string fileName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(folderName);
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

            using (FileStream downloadFileStream = File.OpenWrite(localFilePath))
            {
                var download = await blobClient.DownloadAsync();
                await download.Value.Content.CopyToAsync(downloadFileStream);
                downloadFileStream.Close();
            }
        }

        public void ListFiles(string FolderName)
        {
            throw new NotImplementedException();
        }

        public async Task UploadFile(string folderName, string localFilePath, string fileName = "")
        {
            //BlobContainerClient containerClient = await _blobServiceClient.CreateBlobContainerAsync(folderName);
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(folderName);
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

            using (FileStream uploadFileStream = File.OpenRead(localFilePath))
            {
                await blobClient.UploadAsync(uploadFileStream);
                uploadFileStream.Close();
            }
        }
    }
}
