package com.jack.read;

import org.apache.ibatis.cursor.Cursor;

public interface UserMapper {
    Cursor<User> queryUsers();
}
