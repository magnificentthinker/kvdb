#ifndef STORAGE_KVDB_WRITABLEFILE_H_
#define STORAGE_KVDB_WRITABLEFILE_H_
#include <fstream>

namespace kvdb
{
    class WritableFile
    {
    private:
        const std::string name_;
        std::ofstream dest_;

    public:
        WritableFile(const std::string &name) : name_(name)
        {

            // 打开文件流
            dest_.open(name_);
            if (!dest_.is_open())
            {
                std::cerr << "Failed to open file: " << name_ << std::endl;
            }
        }

        ~WritableFile()
        {
            // 关闭文件流
            dest_.close();
        }

        // 添加一个方法来写入数据 true写入成功，flase写入失败
        bool Append(const char *data, size_t size)
        {
            dest_.write(data, size);
            if (dest_.fail())
            {
                // 写入失败
                return false;
            }
            return true;
        }
    };
}
#endif