#include <Poco/Util/ServerApplication.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/URI.h>
#include <iostream>
#include <unordered_set>
#include "json.hpp"
#include <vector>
#include <string>
#include "Database.h"
using json = nlohmann::json;

class PostImportsHandler : public Poco::Net::HTTPRequestHandler {
public:
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        std::unordered_set<std::string> cur_ids; //ids that received in this request 
        std::unordered_set<std::string> cur_folders; //folder's ids from this request

        json data;
        try {
            data = json::parse(req.stream());
        }
        catch (...) {
            sendError400(res);
            return;
        }
        if (!data.contains("items") || data["items"].is_null() || !data.contains("updateDate") || data["updateDate"].is_null()){
            //|| !checkDate(data["updateDate"])) {
            sendError400(res);
            return;
        }
        else {
            std::string date = data["updateDate"];

            for (auto item = data["items"].begin(); item != data["items"].end(); item++) {

                if (!item->contains("id") || (*item)["id"].is_null()) {
                    sendError400(res);
                    return;
                }
                if (!item->contains("type") || !((*item)["type"] == "FOLDER" || (*item)["type"] == "FILE")) {
                    sendError400(res);
                    return;
                }
                if ((*item)["type"] == "FOLDER") {
                    if (item->contains("url") && !(*item)["url"].is_null()) {
                        sendError400(res);
                        return;
                    }
                    if (item->contains("size") && !(*item)["size"].is_null()) {
                        sendError400(res);
                        return;
                    }
                }
                if ((*item)["type"] == "FILE") {
                    if (item->contains("url") && (*item)["url"].size() > 255) {
                        sendError400(res);
                        return;
                    }
                    if (!item->contains("size") || (*item)["size"].is_null() || (*item)["size"] <= 0) {
                        sendError400(res);
                        return;
                    }
                }

                if (cur_ids.find((*item)["id"]) != cur_ids.end()) {
                    sendError400(res);
                    return;
                }
                cur_ids.insert((*item)["id"]);

                if ((*item)["type"] == "FOLDER") cur_folders.insert((*item)["id"]);


                std::string id = (*item)["id"];
                std::string type = (*item)["type"];

                mysqlx::Row itemInDb = Database::execute("SELECT type FROM item WHERE id = \'" + id + "\'").fetchOne();
                if (!itemInDb.isNull()) {
                    if ((std::string)itemInDb.get(0) != type) {
                        sendError400(res); 
                        return;
                    }
                }
            }

            for (auto item = data["items"].begin(); item != data["items"].end(); item++) {

                if (item->contains("parentId") && !(*item)["parentId"].is_null()) {
                    std::string parent = (*item)["parentId"];

                    if (cur_folders.find(parent) == cur_folders.end()) {
                        mysqlx::Row parentInDb = Database::execute("SELECT type FROM item WHERE id = \'" + parent + "\'").fetchOne();
                        if (parentInDb.isNull()) {
                            sendError400(res);
                            return;
                        }
                        else {
                            if ((std::string)parentInDb.get(0) == "FILE") {
                                sendError400(res);
                                return;
                            }
                        }
                    }
                }
            }

            for (auto item = data["items"].begin(); item != data["items"].end(); item++) {
                std::string columns = "", values = "", update = "";
                columns += "id";
                values += "\'" + (std::string)(*item)["id"] + "\'";

                columns += ", updateDate";
                values += ", \'" + date + "\'";
                update += "updateDate = \'" + date + "\'";

                if (item->contains("url") && !(*item)["url"].is_null()) {
                    columns += ", url";
                    values += ", \'" + (std::string)(*item)["url"] + "\'";
                    update += ", url = \'" + (std::string)(*item)["url"] + "\'";
                }

                if (item->contains("parentId") && !(*item)["parentId"].is_null()) {
                    columns += ", parentId";
                    values += ", \'" + (std::string)(*item)["parentId"] + "\'";
                    update += ", parentId = \'" + (std::string)(*item)["parentId"] + "\'";
                }

                columns += ", type";
                values += ", \'" + (std::string)(*item)["type"] + "\'";

                if (item->contains("size") && !(*item)["size"].is_null()) {
                    columns += ", size";
                    values += ", " + std::to_string((int)(*item)["size"]);
                    update += ", size = " + std::to_string((int)(*item)["size"]);
                }

                mysqlx::Row inDb = Database::execute("SELECT * FROM item WHERE id = \'" + (std::string)(*item)["id"] + "\'").fetchOne();

                if (inDb.isNull()) {
                     Database::execute("INSERT INTO item (" + columns + ") VALUES (" + values + ")");
                }
                else Database::execute("UPDATE item SET " + update + " WHERE id = \'" + (std::string)(*item)["id"] + "\'");

                Database::execute("INSERT INTO history (id, updateDate) VALUES (\'" + (std::string)(*item)["id"] + "\', \'" + date + "\')");
            }

            res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            res.send().flush();

        }
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

class DeleteHandler : public Poco::Net::HTTPRequestHandler {
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        Poco::URI uri(req.getURI());
        std::vector<std::string> path;
        uri.getPathSegments(path);
        std::string id = path[1];

        std::vector<std::pair<std::string, std::string>> params = uri.getQueryParameters();
        if (params.size() != 1 || params[0].first != "date") { //|| !checkDate(params[0].second))
            sendError400(res);
            return;
        }

        mysqlx::Row inDb = Database::execute("SELECT * FROM item WHERE id = \'" + id + "\'").fetchOne();
        if (inDb.isNull()) {
            sendError404(res);
            return;
        }

        std::unordered_set<std::string> to_delete;
        to_delete.insert(id);
        while (!to_delete.empty()) {
            id = *(to_delete.begin());
            to_delete.erase(id);
            Database::execute("DELETE FROM item WHERE id = \'" + id + "\'");
            std::list<mysqlx::Row> rows = Database::execute("SELECT id FROM item WHERE parentId = \'" + id + "\'").fetchAll();
            for (auto line: rows) {
                to_delete.insert((std::string)line.get(0));
            }
        }

        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        res.send().flush();
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

class GetNodesHandler :public Poco::Net::HTTPRequestHandler {
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        Poco::URI uri(req.getURI());
        std::vector<std::string> path;
        uri.getPathSegments(path);

        std::string id = path[1];

        mysqlx::Row infoNode = Database::execute("SELECT * FROM item WHERE id = \'" + id + "\'").fetchOne();
        if (infoNode.isNull()) {
            sendError404(res);
            return;
        }

        int buf;
        json reply = addInfo(id, buf);
        std::ostream& out = res.send(); 
        out << reply;
        out.flush();
        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    }
private:
    json addInfo(std::string id, int& size) {
        json reply;
        mysqlx::Row infoNode = Database::execute("SELECT * FROM item WHERE id = \'" + id + "\'").fetchOne();
        reply["id"] = id;

        if (infoNode.get(1).isNull()) reply["url"] = nullptr; 
        else reply["url"] = infoNode.get(1);

        if (infoNode.get(2).isNull()) reply["parentId"] = nullptr;
        else reply["parentId"] = infoNode.get(2);

        reply["type"] = infoNode.get(3);

        reply["date"] = infoNode.get(5);

        if (reply["type"] == "FILE")
        {
            reply["size"] = (int)infoNode.get(4);
            reply["children"] = nullptr;
            size = (int)infoNode.get(4);
        }
        else {
            size = 0; 
            std::list<json> children_list;
            std::list<mysqlx::Row> children = Database::execute("SELECT id FROM item WHERE parentId = \'" + id + "\'").fetchAll();
            for (auto child : children) {
                int child_size;
                children_list.push_back(addInfo((std::string)child.get(0), child_size));
                size += child_size;
            }
            reply["size"] = size;
            reply["children"] = children_list;
        }
        return reply;
    }
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
        Poco::URI uri(request.getURI());
        std::vector<std::string> path; 
        uri.getPathSegments(path);

        if (request.getMethod() == "POST" && path.size() == 1 && path[0] == "imports") return new PostImportsHandler();
        else if (request.getMethod() == "DELETE" && path.size() == 2 && path[0] == "delete") return new DeleteHandler();
        else if (request.getMethod() == "GET" && path.size() == 2 && path[0] == "nodes") return new GetNodesHandler();
		return nullptr;
	}

};

class Listener : public Poco::Util::ServerApplication {
	virtual int main(const std::vector<std::string>& args) {
        Database::start();
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

