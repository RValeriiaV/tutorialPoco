#pragma once

#include <mysqlx/xdevapi.h>
#include <mysql/jdbc.h>

class Database {
public:
    static mysqlx::Session sess;
    static mysqlx::SqlResult execute(std::string command) {
        return sess.sql(command).execute();
    }
    static void start() {
        sess
            .sql("CREATE TABLE IF NOT EXISTS item(id VARCHAR(255), url text, parentId text, type varchar(6) NOT NULL, size bigint, updateDate DATETIME NOT NULL, PRIMARY KEY(id));")
            .execute();

        sess
            .sql((std::string)"CREATE TABLE IF NOT EXISTS history(history_id bigint unsigned PRIMARY KEY AUTO_INCREMENT, id VARCHAR(255), " + 
                "url text, parentId text, type varchar(6) NOT NULL, size bigint, updateDate DATETIME NOT NULL," +
                "FOREIGN KEY(id) REFERENCES item(id) ON DELETE CASCADE); ")
            .execute();
    }
};
mysqlx::Session Database::sess("localhost", 33060, "root", "250520", "test");
