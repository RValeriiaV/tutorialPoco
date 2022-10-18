#pragma once

#include <mysqlx/xdevapi.h>
#include <mysql/jdbc.h>

class Database {
public:
    static mysqlx::Session sess;
    static mysqlx::SqlResult execute(std::string command) {
        return sess.sql(command).execute();
    }
};
mysqlx::Session Database::sess("localhost", 33060, "root", "250520", "test");
