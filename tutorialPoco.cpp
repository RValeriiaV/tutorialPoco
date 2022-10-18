#include <Poco/Util/ServerApplication.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <iostream>
#include <vector>
#include <string>
#include "Database.h"

class PostImportsHandler : public Poco::Net::HTTPRequestHandler {
public:
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        Database::execute("INSERT INTO tre123 VALUES(2)");
        std::cout << "ok";
        //Listener::useParser();
        //unordered_set<std::string> cur_ids; //ids that received in this request 
        //unordered_set<std::string> cur_folders; //folder's ids from this request
        //Database db;

        //json data;
        //try {
        //    data = json::parse(req.stream());
        //}
        //catch (...) {
        //    sendError400(res);
        //    return;
        //}
        //if (!data.contains("items") || data["items"].is_null() || !data.contains("updateDate") || data["updateDate"].is_null()){
        //    //|| !checkDate(data["updateDate"])) {
        //    sendError400(res);
        //    return;
        //}
        //else {
        //    std::string date = data["updateDate"];

        //    for (auto item = data["items"].begin(); item != data["items"].end(); item++) {

        //        if (!item->contains("id") || (*item)["id"].is_null()) {
        //            sendError400(res);
        //            return;
        //        }
        //        if (!item->contains("type") || !((*item)["type"] == "FOLDER" || (*item)["type"] == "FILE")) {
        //            sendError400(res);
        //            return;
        //        }
        //        if ((*item)["type"] == "FOLDER") {
        //            if (item->contains("url") && !(*item)["url"].is_null()) {
        //                sendError400(res);
        //                return;
        //            }
        //            if (item->contains("size") && !(*item)["size"].is_null()) {
        //                sendError400(res);
        //                return;
        //            }
        //        }
        //        if ((*item)["type"] == "FILE") {
        //            if (item->contains("url") && (*item)["url"].size() > 255) {
        //                sendError400(res);
        //                return;
        //            }
        //            if (!item->contains("size") || (*item)["size"].is_null() || (*item)["size"] <= 0) {
        //                sendError400(res);
        //                return;
        //            }
        //        }

        //        if (cur_ids.find((*item)["id"]) != cur_ids.end()) {
        //            sendError400(res);
        //            return;
        //        }
        //        cur_ids.insert((*item)["id"]);

        //        if ((*item)["type"] == "FOLDER") cur_folders.insert((*item)["id"]);


        //        std::string id = (*item)["id"];
        //        std::string type = (*item)["id"];

        //        SqlResult itemInDb = db.execute("SELECT type FROM item WHERE id = \'" + id + "\'");
        //        std::list<Row> rows = itemInDb.fetchAll();
        //        if (!rows.empty()) {
        //            mysqlx::abi2::r0::string typeInDb = rows.begin()->get(0);
        //            if (typeInDb != type) {
        //                sendError400(res); 
        //                return;
        //            }
        //        }
        //    }

        //    for (auto item = data["items"].begin(); item != data["items"].end(); item++) {
        //        if (item->contains("parentId") && !(*item)["parentId"].is_null()) {
        //            std::string parent = (*item)["parentId"];
        //            if (cur_folders.find(parent) == cur_folders.end()) {
        //                SqlResult parentInDb = db.execute("SELECT type FROM item WHERE id = \'" + parent + "\'");
        //                std::list<Row> rows = parentInDb.fetchAll();
        //                if (rows.empty()) {
        //                    sendError400(res);
        //                    return;
        //                }
        //                else {
        //                    mysqlx::abi2::r0::string typeInDb = rows.begin()->get(0);
        //                    if (typeInDb == "FILE") {
        //                        sendError400(res);
        //                        return;
        //                    }
        //                }
        //            }
        //        }
        //    }

        //    for (auto item = data["items"].begin(); item != data["items"].end(); item++) {
        //        std::string columns = "", values = "", update = "";
        //        columns += "id";
        //        values += "\'" + (std::string)(*item)["id"] + "\'";

        //        columns += ", updateDate";
        //        values += ", \'" + date + "\'";
        //        update += "updateDate = \'" + date + "\'";

        //        if (item->contains("url") && !(*item)["url"].is_null()) {
        //            columns += ", url";
        //            values += ", \'" + (std::string)(*item)["url"] + "\'";
        //            update += ", url = \'" + (std::string)(*item)["url"] + "\'";
        //        }

        //        if (item->contains("parentId") && !(*item)["parentId"].is_null()) {
        //            columns += ", parentId";
        //            values += ", \'" + (std::string)(*item)["parentId"] + "\'";
        //            update += ", parentId = \'" + (std::string)(*item)["parentId"] + "\'";
        //        }

        //        columns += ", type";
        //        values += ", \'" + (std::string)(*item)["type"] + "\'";

        //        if (item->contains("size") && !(*item)["size"].is_null()) {
        //            columns += ", size";
        //            values += ", " + (*item)["size"];
        //            update += ", size = " + std::to_string((int)(*item)["size"]);
        //        }

        //        SqlResult inDb = db.execute("SELECT * FROM item WHERE id = \'" + (std::string)(*item)["id"] + "\'");
        //        std::list<Row> rows = inDb.fetchAll();

        //        if (rows.empty()) {
        //            db.execute("INSERT INTO item (" + columns + ") VALUES (" + values + ")");
        //        }
        //        else db.execute("UPDATE item SET " + update + "WHERE id = \'" + (std::string)(*item)["id"] + "\'");

        //        db.execute("INSERT INTO history (id, updateDate) VALUES (\'" + (std::string)(*item)["id"] + "\', \'" + date + "\')");
        //        
        //        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        //    }

        //}

    }
private:
    void sendError400(Poco::Net::HTTPServerResponse& resp) {
        resp.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        std::ostream& out = resp.send();
        out << R"("code": 400, "message": "Validation Failed")";
        out.flush();
    }
    void sendError404(Poco::Net::HTTPServerResponse& resp) {
        resp.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        std::ostream& out = resp.send();
        out << R"("code": 404, "message": "Item not found")";
        out.flush();
    }
};

class CommonRequestHandler : public Poco::Net::HTTPRequestHandlerFactory {

public:
	CommonRequestHandler() {}

	virtual Poco::Net::HTTPRequestHandler* createRequestHandler(const Poco::Net::HTTPServerRequest& request) {
        if (request.getMethod() == "POST" && request.getURI() == "/imports") return new PostImportsHandler();
		return nullptr; // Does everybody use C++11?
	}

};

class Listener : public Poco::Util::ServerApplication {
	virtual int main(const std::vector<std::string>& args) {
		Database::execute("INSERT INTO tre123 VALUES(1)");
		Poco::Net::HTTPServerParams* params = new Poco::Net::HTTPServerParams();
		params->setMaxQueued(50);
		params->setMaxThreads(4);

		Poco::Net::ServerSocket socket(8765); // argument is a port 

		Poco::Net::HTTPServer server(new CommonRequestHandler(),
			socket,
			params);

		server.start();
		waitForTerminationRequest();
		server.stop();

		return 0;
	}
};

POCO_SERVER_MAIN(Listener)

