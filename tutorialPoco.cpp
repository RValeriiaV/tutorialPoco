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

class Util {
public:
    static void updateInfoFolder(std::unordered_set<std::string>& folders, std::string date) {
        std::unordered_set<std::string> allParents;

        while (!folders.empty()) {
            std::string cur_parent = *folders.begin();
            folders.erase(cur_parent);
            allParents.insert(cur_parent);

            mysqlx::Row grandpa = Database::execute("SELECT parentId FROM item WHERE id = \'" + cur_parent + "\'").fetchOne();
            while (!grandpa.isNull() && !grandpa.get(0).isNull() && allParents.find((std::string)grandpa.get(0)) == allParents.end()) {
                allParents.insert((std::string)grandpa.get(0));
                grandpa = Database::execute("SELECT parentId FROM item WHERE id = \'" + (std::string)grandpa.get(0) + "\'").fetchOne();
            }
        }
        for (auto parent : allParents) {
            int size = getSize(parent);
            Database::execute("UPDATE item SET size = " + std::to_string(size) + " WHERE id = \'" + parent + "\'");

            mysqlx::Row infoParent = Database::execute("SELECT url, parentId FROM item WHERE id = \'" + parent + "\'").fetchOne();

            std::string columns = "", values = "";
            columns += "id";
            values += "\'" + parent + "\'";

            columns += ", updateDate";
            values += ", \'" + date + "\'";

            if (!infoParent.get(0).isNull()) {
                columns += ", url";
                values += ", \'" + (std::string)infoParent.get(0) + "\'";
            }

            if (!infoParent.get(1).isNull()) {
                columns += ", parentId";
                values += ", \'" + (std::string)infoParent.get(1) + "\'";
            }

            columns += ", type";
            values += ", \'FOLDER\'";

            columns += ", size";
            values += ", " + std::to_string(size);

            Database::execute("INSERT INTO history (" + columns + ") VALUES (" + values + ")");
        }
    }
    int static getSize(std::string id) {
        mysqlx::Row infoItem = Database::execute("SELECT type, size FROM item WHERE id = \'" + id + "\'").fetchOne();

        if ((std::string)infoItem.get(0) == "FILE")
            return (int)infoItem.get(1);

        int size = 0;
        std::list<mysqlx::Row> children = Database::execute("SELECT id FROM item WHERE parentId = \'" + id + "\'").fetchAll();
        for (auto child : children) {
            size += getSize((std::string)child.get(0));
        }
        return size;
    }
    static void sendError400(Poco::Net::HTTPServerResponse& resp) {
        resp.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        std::ostream& out = resp.send();
        out << R"("code": 400, "message": "Validation Failed")";
        out.flush();
    }
    static void sendError404(Poco::Net::HTTPServerResponse& resp) {
        resp.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        std::ostream& out = resp.send();
        out << R"("code": 404, "message": "Item not found")";
        out.flush();
    }
};

class PostImportsHandler : public Poco::Net::HTTPRequestHandler {
public:
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        std::unordered_set<std::string> cur_ids;
        std::unordered_set<std::string> folders;

        json data;
        try {
            data = json::parse(req.stream());
        }
        catch (...) {
            Util::sendError400(res);
            return;
        }
        if (!data.contains("items") || data["items"].is_null() || !data.contains("updateDate") || data["updateDate"].is_null()){
            //|| !checkDate(data["updateDate"])) {
            Util::sendError400(res);
            return;
        }
        
        std::string date = data["updateDate"];
        date.pop_back();


        for (auto item = data["items"].begin(); item != data["items"].end(); item++) {

            if (!item->contains("id") || (*item)["id"].is_null()) {
                Util::sendError400(res);
                return;
            }
            if (!item->contains("type") || !((*item)["type"] == "FOLDER" || (*item)["type"] == "FILE")) {
                Util::sendError400(res);
                return;
            }
            if ((*item)["type"] == "FOLDER") {
                if (item->contains("url") && !(*item)["url"].is_null()) {
                    Util::sendError400(res);
                    return;
                }
                if (item->contains("size") && !(*item)["size"].is_null()) {
                    Util::sendError400(res);
                    return;
                }
            }
            if ((*item)["type"] == "FILE") {
                if (item->contains("url") && (*item)["url"].size() > 255) {
                    Util::sendError400(res);
                    return;
                }
                if (!item->contains("size") || (*item)["size"].is_null() || (*item)["size"] <= 0) {
                    Util::sendError400(res);
                    return;
                }
            }

            if (cur_ids.find((*item)["id"]) != cur_ids.end()) {
                Util::sendError400(res);
                return;
            }
            cur_ids.insert((*item)["id"]);

            if ((*item)["type"] == "FOLDER") folders.insert((*item)["id"]);


            std::string id = (*item)["id"];
            std::string type = (*item)["type"];

            mysqlx::Row itemInDb = Database::execute("SELECT type FROM item WHERE id = \'" + id + "\'").fetchOne();
            if (!itemInDb.isNull()) {
                if ((std::string)itemInDb.get(0) != type) {
                    Util::sendError400(res);
                    return;
                }
            }
        }

        for (auto item = data["items"].begin(); item != data["items"].end(); item++) {

            if (item->contains("parentId") && !(*item)["parentId"].is_null()) {
                std::string parent = (*item)["parentId"];

                if (folders.find(parent) == folders.end()) {
                    mysqlx::Row parentInDb = Database::execute("SELECT type FROM item WHERE id = \'" + parent + "\'").fetchOne();
                    if (parentInDb.isNull()) {
                        Util::sendError400(res);
                        return;
                    }
                    else {
                        if ((std::string)parentInDb.get(0) == "FILE") {
                            Util::sendError400(res);
                            return;
                        }
                    }
                    folders.insert(parent);
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

            mysqlx::Row inDb = Database::execute("SELECT id FROM item WHERE id = \'" + (std::string)(*item)["id"] + "\'").fetchOne();

            if (inDb.isNull()) {
                Database::execute("INSERT INTO item (" + columns + ") VALUES (" + values + ")");
            }
            else Database::execute("UPDATE item SET " + update + " WHERE id = \'" + (std::string)(*item)["id"] + "\'");

            if ((std::string)(*item)["type"] == "FILE")
                Database::execute("INSERT INTO history (" + columns + ") VALUES (" + values + ")");
        }
      
        
        Util::updateInfoFolder(folders, date);

        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        res.send().flush();
    }   
};

class DeleteHandler : public Poco::Net::HTTPRequestHandler {
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");
        std::unordered_set<std::string> folders;

        Poco::URI uri(req.getURI());
        std::vector<std::string> path;
        uri.getPathSegments(path);
        std::string id = path[1];

        std::vector<std::pair<std::string, std::string>> params = uri.getQueryParameters();
        if (params.size() != 1 || params[0].first != "date") { //|| !checkDate(params[0].second))
            Util::sendError400(res);
            return;
        }

        mysqlx::Row inDb = Database::execute("SELECT id, parentId FROM item WHERE id = \'" + id + "\'").fetchOne();
        if (inDb.isNull()) {
            Util::sendError404(res);
            return;
        }

        if (!inDb.get(1).isNull()) folders.insert((std::string)inDb.get(1));

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

        std::string date = (std::string)params[0].second; 
        date.pop_back();
        Util::updateInfoFolder(folders, date);

        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        res.send().flush();
   }
};

class GetNodesHandler :public Poco::Net::HTTPRequestHandler {
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        Poco::URI uri(req.getURI());
        std::vector<std::string> path;
        uri.getPathSegments(path);

        std::string id = path[1];

        mysqlx::Row infoNode = Database::execute("SELECT id FROM item WHERE id = \'" + id + "\'").fetchOne();
        if (infoNode.isNull()) {
            Util::sendError404(res);
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
        mysqlx::Row infoNode = 
            Database::execute("SELECT url, parentId, type, , size, DATE_FORMAT(updateDate,'%Y-%m-%dT%H:%m:%s') AS date_formatted FROM item WHERE id = \'" + id + "\'")
            .fetchOne();
        reply["id"] = id;

        if (infoNode.get(0).isNull()) reply["url"] = nullptr; 
        else reply["url"] = infoNode.get(0);

        if (infoNode.get(1).isNull()) reply["parentId"] = nullptr;
        else reply["parentId"] = infoNode.get(1);

        reply["type"] = infoNode.get(2);

        reply["date"] = (std::string)infoNode.get(4) + ".000Z";

        if (reply["type"] == "FILE")
        {
            reply["size"] = (int)infoNode.get(3);
            reply["children"] = nullptr;
            size = (int)infoNode.get(3);
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
};

class GetUpdatesHandler : public Poco::Net::HTTPRequestHandler {
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        Poco::URI uri(req.getURI());
        std::vector<std::pair<std::string, std::string>> params = uri.getQueryParameters();
        if (params.size() != 1 || params[0].first != "date") { //|| !checkDate(params[0].second))
            Util::sendError400(res);
            return;
        }
        std::string date = params[0].second;
        date.pop_back();
        
        std::list<mysqlx::Row> updatedIds =
            Database::execute("SELECT id FROM history WHERE timestampdiff(second, updateDate, \'" + (std::string)date + "\') <= 24 * 3600")
            .fetchAll();

        std::set<std::string> ids;
        for (auto updation : updatedIds) {
            ids.insert((std::string)updation.get(0));
        }

        std::list<json> items;
        for (auto id : ids) {
            json item; 
            mysqlx::Row infoItem =
                Database::execute("SELECT url, parentId, type, size, DATE_FORMAT(updateDate,'%Y-%m-%dT%H:%m:%s') AS date_formatted FROM item WHERE id = \'"
                    + id + "\'").fetchOne();
            item["id"] = id;

            if (infoItem.get(0).isNull()) item["url"] = nullptr;
            else item["url"] = infoItem.get(0);

            if (infoItem.get(1).isNull()) item["parentId"] = nullptr;
            else item["parentId"] = infoItem.get(1);

            item["type"] = infoItem.get(2);

            if (infoItem.get(3).isNull()) item["size"] = nullptr;
            else item["size"] = (int)infoItem.get(3);

            item["date"] = (std::string)infoItem.get(4) + ".000Z";

            items.push_back(item);
        }

        json reply;
        reply["items"] = items;

        res.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        std::ostream& out = res.send(); 
        out << reply;
        out.flush();

    }
};

class GetHistoryHandler :public Poco::Net::HTTPRequestHandler {
    virtual void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setContentType("application/json");

        Poco::URI uri(req.getURI());
        std::vector<std::string> path;
        uri.getPathSegments(path);
        std::string id = path[1];

        std::vector<std::pair<std::string, std::string>> params = uri.getQueryParameters();

        std::string dateStart, dateEnd; 
        bool start = false, end = false;

        if (params.size() == 0);
        else if (params.size() == 1) {
            if (!dates(params, dateStart, dateEnd, start, end, 0)) {
                Util::sendError400(res);
                return;
            }
        }
        else if (params.size() == 2) {
            if (params[0].first == params[1].first) {
                Util::sendError400(res);
                return;
            }
            if (!dates(params, dateStart, dateEnd, start, end, 0)) {
                Util::sendError400(res);
                return;
            }
            if (!dates(params, dateStart, dateEnd, start, end, 1)) {
                Util::sendError400(res);
                return;
            }
        }
        else {
            Util::sendError400(res); 
            return;
        }

        mysqlx::Row presence = Database::execute("SELECT id FROM item WHERE id = \'" + id + "\'").fetchOne();
        if (presence.isNull()) {
            Util::sendError404(res);
            return;
        }

        std::string clauses = "WHERE id = \'" + id + "\'";
        if (start && end)
            clauses = "AND updateDate >= \'" + dateStart + "\' AND updateDate < \'" + dateEnd + "\'";
        else if (start)
            clauses = "AND updateDate >= \'" + dateStart + "\'";
        else if (end)
            clauses = "AND updateDate < \'" + dateEnd + "\'";

        std::list<mysqlx::Row> historyLines =
            Database::execute("SELECT url, parentId, type, size, DATE_FORMAT(updateDate,'%Y-%m-%dT%H:%m:%s') AS date_formatted FROM history " 
                 + clauses + "ORDER BY updateDate")
            .fetchAll();

        std::list<json> history;
        for (auto line : historyLines) {
            json cur_line;
            cur_line["id"] = id;
            if (!line.get(0).isNull()) cur_line["url"] = line.get(0);
            else cur_line["url"] = nullptr;
            if (!line.get(1).isNull()) cur_line["parentId"] = line.get(1);
            else cur_line["parentId"] = nullptr;
            cur_line["type"] = line.get(2);
            if (!line.get(3).isNull()) cur_line["size"] = (int)line.get(3);
            else cur_line["size"] = nullptr;
            cur_line["date"] = (std::string)line.get(4) + ".000Z";
            history.push_back(cur_line);
        }
        json reply; 
        reply["items:"] = history;

        res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        std::ostream& out = res.send(); 
        out << reply;
        out.flush();
    }
    bool dates(std::vector<std::pair<std::string, std::string>>& params, std::string& dateStart, std::string& dateEnd, bool start, bool end, int i) {
        if (params[i].first == "dateStart") {
            dateStart = params[i].second;
            //if (!checkDate(dateStart) return false;
            dateStart.pop_back();
            start = true;
            return true;
        }
        if (params[i].first == "dateEnd") {
            dateEnd = params[i].second;
            //if (!checkDate(dateEnd) return false;
            dateEnd.pop_back();
            end = true;
            return true;
        }
        return false;
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
        else if (request.getMethod() == "GET" && path.size() == 1 && path[0] == "updates") return new GetUpdatesHandler();
        else if (request.getMethod() == "GET" && path.size() == 3 && path[0] == "node" && path[2] == "history") return new GetHistoryHandler();
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

