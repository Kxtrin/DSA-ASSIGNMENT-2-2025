import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerinax/kafka;

configurable string dbHost = "mysql";
configurable int dbPort = 3306;
configurable string dbUser = "root";
configurable string dbPassword = "root123";
configurable string kafkaBootstrap = "kafka:9092";

final mysql:Client usersDb = check new(
    host = dbHost, port = dbPort, user = dbUser,
    password = dbPassword, database = "dsa_users"
);

final mysql:Client ticketsDb = check new(
    host = dbHost, port = dbPort, user = dbUser,
    password = dbPassword, database = "dsa_tickets"
);

final mysql:Client paymentsDb = check new(
    host = dbHost, port = dbPort, user = dbUser,
    password = dbPassword, database = "dsa_payments"
);

final mysql:Client routesDb = check new(
    host = dbHost, port = dbPort, user = dbUser,
    password = dbPassword, database = "dsa_routes"
);

final mysql:Client notificationsDb = check new(
    host = dbHost, port = dbPort, user = dbUser,
    password = dbPassword, database = "dsa_notifications"
);

kafka:ProducerConfiguration producerConfig = {
    clientId: "admin-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new(kafkaBootstrap, producerConfig);

type Health record {|
    string status;
    string 'service;
    time:Utc timestamp;
|};

type DashboardStats record {|
    int total_users;
    int total_tickets;
    int active_routes;
    decimal total_revenue;
|};

type Route record {|
    int route_id;
    string route_name;
    string start_point;
    string end_point;
    decimal distance_km;
    decimal base_price;
    boolean is_active;
    string created_at;
|};

service /admin on new http:Listener(8085) {
    
    resource function get health() returns Health {
        return {
            status: "UP",
            'service: "Admin Service",
            timestamp: time:utcNow()
        };
    }

    resource function get dashboard/stats() returns DashboardStats|error {
        stream<record {int total;}, error?> userCountStream = usersDb->query(
            `SELECT COUNT(*) as total FROM users WHERE is_active = TRUE`
        );
        record {|record {int total;} value;|}? userResult = check userCountStream.next();
        check userCountStream.close();
        int totalUsers = userResult is record {|record {int total;} value;|} ? userResult.value.total : 0;

        stream<record {int total;}, error?> ticketCountStream = ticketsDb->query(
            `SELECT COUNT(*) as total FROM tickets`
        );
        record {|record {int total;} value;|}? ticketResult = check ticketCountStream.next();
        check ticketCountStream.close();
        int totalTickets = ticketResult is record {|record {int total;} value;|} ? ticketResult.value.total : 0;

        stream<record {int total;}, error?> routeCountStream = routesDb->query(
            `SELECT COUNT(*) as total FROM routes WHERE is_active = TRUE`
        );
        record {|record {int total;} value;|}? routeResult = check routeCountStream.next();
        check routeCountStream.close();
        int activeRoutes = routeResult is record {|record {int total;} value;|} ? routeResult.value.total : 0;

        stream<record {decimal total;}, error?> revenueStream = paymentsDb->query(
            `SELECT COALESCE(SUM(amount), 0) as total FROM payments WHERE payment_status = 'paid'`
        );
        record {|record {decimal total;} value;|}? revenueResult = check revenueStream.next();
        check revenueStream.close();
        decimal totalRevenue = revenueResult is record {|record {decimal total;} value;|} ? revenueResult.value.total : 0.0;

        return {
            total_users: totalUsers,
            total_tickets: totalTickets,
            active_routes: activeRoutes,
            total_revenue: totalRevenue
        };
    }

    resource function get users() returns json|error {
        stream<record {}, error?> userStream = usersDb->query(
            `SELECT user_id, user_type, first_name, last_name, email, phone, 
                    is_active, created_at
             FROM users ORDER BY created_at DESC LIMIT 100`
        );

        json[] users = [];
        check from record {} user in userStream
            do {
                users.push(user.toJson());
            };

        return users;
    }

    resource function put users/[int id]/deactivate() returns http:Response|error {
        http:Response resp = new;

        _ = check usersDb->execute(
            `UPDATE users SET is_active = FALSE WHERE user_id = ${id}`
        );

        resp.statusCode = 200;
        resp.setJsonPayload({"message": "User deactivated successfully"});
        log:printInfo("User deactivated", user_id = id);
        return resp;
    }

    // ==================== ROUTE MANAGEMENT ====================
    
    resource function get routes() returns Route[]|error {
        stream<Route, error?> routeStream = routesDb->query(
            `SELECT route_id, route_name, start_point, end_point, 
                    distance_km, base_price, is_active, created_at
             FROM routes ORDER BY created_at DESC`
        );

        return from Route r in routeStream select r;
    }

    resource function post routes(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        string routeName = check req.route_name;
        string startPoint = check req.start_point;
        string endPoint = check req.end_point;
        decimal distanceKm = check req.distance_km;
        decimal basePrice = check req.base_price;

        var result = routesDb->execute(
            `INSERT INTO routes (route_name, start_point, end_point, distance_km, base_price)
             VALUES (${routeName}, ${startPoint}, ${endPoint}, ${distanceKm}, ${basePrice})`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        json event = {
            "event_type": "route.created",
            "route_name": routeName,
            "start_point": startPoint,
            "end_point": endPoint,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "route.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Route created successfully"});
        log:printInfo("Route created by admin", route = routeName);
        return resp;
    }

    resource function put routes/[int id](@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        string routeName = check req.route_name;
        string startPoint = check req.start_point;
        string endPoint = check req.end_point;
        decimal distanceKm = check req.distance_km;
        decimal basePrice = check req.base_price;

        var result = routesDb->execute(
            `UPDATE routes 
             SET route_name = ${routeName}, 
                 start_point = ${startPoint},
                 end_point = ${endPoint},
                 distance_km = ${distanceKm},
                 base_price = ${basePrice}
             WHERE route_id = ${id}`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        json event = {
            "event_type": "route.updated",
            "route_id": id,
            "route_name": routeName,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "route.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 200;
        resp.setJsonPayload({"message": "Route updated successfully"});
        log:printInfo("Route updated", route_id = id);
        return resp;
    }

    resource function delete routes/[int id]() returns http:Response|error {
        http:Response resp = new;

        var result = routesDb->execute(
            `UPDATE routes SET is_active = FALSE WHERE route_id = ${id}`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        json event = {
            "event_type": "route.deactivated",
            "route_id": id,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "route.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 200;
        resp.setJsonPayload({"message": "Route deactivated successfully"});
        log:printInfo("Route deactivated", route_id = id);
        return resp;
    }

    // ==================== NOTIFICATION MANAGEMENT ====================
    
    resource function post notifications/broadcast(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        string title = check req.title;
        string message = check req.message;
        string notificationType = req.hasKey("notification_type") ? 
            check req.notification_type : "trip_change";

        stream<record {int user_id;}, error?> userStream = usersDb->query(
            `SELECT user_id FROM users WHERE is_active = TRUE`
        );

        int notificationCount = 0;
        check from var user in userStream
            do {
                _ = check notificationsDb->execute(
                    `INSERT INTO notifications (user_id, notification_type, title, message, sent_at)
                     VALUES (${user.user_id}, ${notificationType}, ${title}, ${message}, NOW())`
                );
                notificationCount += 1;
            };

        json event = {
            "event_type": "notification.broadcast",
            "title": title,
            "recipients": notificationCount,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "notification.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 200;
        resp.setJsonPayload({
            "message": "Broadcast notification sent successfully",
            "recipients": notificationCount
        });
        log:printInfo("Broadcast notification sent", recipients = notificationCount);
        return resp;
    }

    resource function post notifications/user/[int userId](@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        string title = check req.title;
        string message = check req.message;
        string notificationType = req.hasKey("notification_type") ? 
            check req.notification_type : "trip_change";

        var result = notificationsDb->execute(
            `INSERT INTO notifications (user_id, notification_type, title, message, sent_at)
             VALUES (${userId}, ${notificationType}, ${title}, ${message}, NOW())`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        json event = {
            "event_type": "notification.sent",
            "user_id": userId,
            "title": title,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "notification.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Notification sent successfully"});
        log:printInfo("User notification sent", user_id = userId);
        return resp;
    }

    resource function get tickets/summary() returns json|error {
        stream<record {string status; int count;}, error?> statusStream = ticketsDb->query(
            `SELECT status, COUNT(*) as count 
             FROM tickets 
             GROUP BY status`
        );

        json[] summary = [];
        check from var s in statusStream
            do {
                summary.push(s.toJson());
            };

        return summary;
    }

    resource function get routes/performance() returns json|error {
        stream<record {}, error?> performanceStream = ticketsDb->query(
            `SELECT t.route_id, COUNT(t.ticket_id) as total_bookings,
                    COALESCE(SUM(t.total_price), 0) as revenue
             FROM tickets t
             GROUP BY t.route_id
             ORDER BY total_bookings DESC`
        );

        json[] performance = [];
        check from record {} route in performanceStream
            do {
                performance.push(route.toJson());
            };

        return performance;
    }
}