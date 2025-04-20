#include "util/writablefile.h"
#include <iostream>
int main()
{
    kvdb::WritableFile file("test");

    enum x
    {
        a = 0x0,
        b = 0x1,
    };
    char buf[1];
    buf[0] = static_cast<char>(x::b & 0xff);

    file.Append(buf, 1);
}