using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CloudPortAPI.Services
{
    public interface IBlobService
    {
        void CreateFolder(string folderName);
        Task UploadFile(string folderName, string localFilePath, string fileName = "");
        void ListFiles(string FolderName);
        Task DownloadFile(string folderName, string localFilePath, string fileName);
        void DeleteFile(string path);
        void DeleteFolder(string path);
    }
}
