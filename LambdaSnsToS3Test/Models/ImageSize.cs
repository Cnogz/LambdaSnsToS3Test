namespace LambdaSnsToS3Test.Models
{
    public class ImageSize
    {
        public ImageSize(string key, int width, int height)
        {
            Key = key;
            Width = width;
            Height = height;
        }
        public string Key { get; set; }
        public int Width { get; set; }
        public int Height { get; set; }
    }
}