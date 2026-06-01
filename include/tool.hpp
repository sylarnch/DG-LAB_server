// tool.hpp
#ifndef TOOL_HPP
#define TOOL_HPP

#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

// 读取配置文件
inline std::map<std::string, std::string> readConfig(const std::string &filename)
{
    std::map<std::string, std::string> result;
    std::ifstream file;
    file.open(filename);
    if (!file.is_open())
    {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return result;
    }
    std::string line;
    while (std::getline(file, line))
    {
        auto pos = line.find(':');
        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        std::cout << "Key: " << key << ", Value: " << value << std::endl;
        result[key] = value;
    }
    return result;
}

// 生成 UUID
inline std::string getUUID(boost::uuids::random_generator &gen)
{
    // 生成 UUID
    boost::uuids::uuid u = gen();

    // 直接输出字符串格式，例如: f81d4fae-7dec-11d0-a765-00a0c91e6bf6
    std::cout << u << std::endl;

    // 如果需要转为 std::string
    std::string s = to_string(u);

    return s;
}

#endif