using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.SNSEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

using LambdaSnsToS3Test.Models;

using Newtonsoft.Json;

using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Processing;

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading.Tasks;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace LambdaSnsToS3Test
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SNS event object and can be used 
        /// to respond to SNS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SNSEvent evnt, ILambdaContext context)
        {
            foreach (var record in evnt.Records)
            {
                await ProcessRecordAsync(record, context);
            }
        }

        private async Task ProcessRecordAsync(SNSEvent.SNSRecord record, ILambdaContext context)
        {
            context.Logger.LogLine($"Processed record {record.Sns.Message}");

            var s3Responses = await GetS3ResponsesAsync(record.Sns.Message, context);

            var imageSizes = new List<ImageSize>
            {
                new ImageSize("small", 400, 400),
                new ImageSize("medium", 1000, 1000),
                new ImageSize("large", 1600, 1600)
            };

            var items =Environment.GetEnvironmentVariables();

            var sizes = items.Keys.Cast<object>().ToDictionary(k => k.ToString(), v => items[v]).Where(s=> s.Key.Contains("Image_Size_"));

            foreach (var response in s3Responses)
            {
                var filePath = Path.GetDirectoryName(response.Key).Replace(Path.DirectorySeparatorChar, '/');
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(response.Key);

                byte[] imageBytes = null;
                MemoryStream _ms = new MemoryStream();

                await using var responseStream = response.ResponseStream;
                responseStream.CopyTo(_ms);

                imageBytes = _ms.ToArray();

                var images = ResizeImg(imageBytes, imageSizes);

                //TODO - FileName Gerekli.
                var zippedFile = CreateZip("can", images);

                var zipFileName = $"{filePath}/{fileNameWithoutExtension}.zip";

                var result = await UploadFileWithTagAsync(response.BucketName, zipFileName, zippedFile, new Dictionary<string, string>());


            }

            await Task.CompletedTask;
        }

        private async Task<bool> UploadFileWithTagAsync(string bucket, string path, byte[] bytes, Dictionary<string, string> tags)
        {
            try
            {
                var transferUtil = new TransferUtility(S3Client);
                using (var stream = new MemoryStream(bytes))
                {
                    await transferUtil.UploadAsync(new TransferUtilityUploadRequest()
                    {
                        BucketName = bucket,
                        Key = path,
                        TagSet = tags.Select(x => new Tag()
                        {
                            Key = x.Key,
                            Value = x.Value
                        }).ToList(),
                        InputStream = stream,
                    });
                }

                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        private async Task<List<GetObjectResponse>> GetS3ResponsesAsync(string message, ILambdaContext context)
        {
            var s3Event = JsonConvert.DeserializeObject<S3Event>(message);

            var result = new List<GetObjectResponse>();

            foreach (var record in s3Event.Records)
            {
                var bucketName = record.S3.Bucket.Name;
                var key = record.S3.Object.Key;

                var request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key
                };

                var s3Response = await S3Client.GetObjectAsync(request);

                if (s3Response?.HttpStatusCode == HttpStatusCode.OK)
                {
                    result.Add(s3Response);
                }
                else
                {
                    context.Logger.LogLine($"S3 Retrieve Object Failed: key-{key}");
                }

            }

            return result;
        }

        public static IDictionary<string, object> Convert<TKey, TValue>(IDictionary<TKey, TValue> genDictionary)
        {
            return genDictionary.Select(kvp => new KeyValuePair<string, object>(kvp.Key.ToString(), (object)kvp.Value)).ToDictionary(x => x.Key, x => x.Value);
        }
        private byte[] CreateZip(string fileName, Dictionary<string, byte[]> images)
        {
            byte[] bytes;
            using (var memoryStream = new MemoryStream())
            {
                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                {
                    foreach (var image in images)
                    {
                        var dataFile = archive.CreateEntry($"/{fileName}_{image.Key}");
                        using (var entryStream = dataFile.Open())
                        {
                            entryStream.Write(image.Value);
                        }
                    }
                }
                bytes = memoryStream.ToArray();
            }

            return bytes;
        }

        private static Dictionary<string, byte[]> ResizeImg(byte[] bytes, List<ImageSize> sizeList)
        {
            var result = new Dictionary<string, byte[]>();

            foreach (var size in sizeList)
            {
                using (Image image = Image.Load(bytes))
                {
                    if (image.Width != size.Width && image.Height != size.Height) image.Mutate(x => x.Resize(size.Width, size.Height));

                    using (var memoryStream = new MemoryStream())
                    {
                        image.Save(memoryStream, new JpegEncoder());
                        var pixelData = memoryStream.ToArray();
                        result.Add(size.Key, pixelData);
                    }
                }
            }

            return result;
        }
    }
}
