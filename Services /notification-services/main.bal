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
configurable string dbName = "dsa_notifications";
configurable string kafkaBootstrap = "kafka:9092";

final mysql:Client db = check new(
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

type Notification record {|
    int notification_id;
    int user_id;
    string notification_type;
    string title;
    string message;
    boolean is_read;
    string created_at;
|};

type NotificationPreference record {|
    int preference_id;
    int user_id;
    boolean email_notifications;
    boolean sms_notifications;
    boolean push_notifications;
|};

type Health record {|
    string status;
    string 'service;
    time:Utc timestamp;
|};

service /notification on new http:Listener(8084) {
    
    resource function get health() returns Health {
        return {
            status: "UP",
            'service: "Notification Service",
            timestamp: time:utcNow()
        };
    }

    // ==================== GET NOTIFICATIONS ====================

    resource function get notifications(int user_id, boolean? unread_only) returns Notification[]|error {
        stream<Notification, error?> notificationStream;
        
        if unread_only == true {
            notificationStream = db->query(
                `SELECT notification_id, user_id, notification_type, title, 
                        message, is_read, created_at
                 FROM notifications 
                 WHERE user_id = ${user_id} AND is_read = FALSE
                 ORDER BY created_at DESC`
            );
        } else {
            notificationStream = db->query(
                `SELECT notification_id, user_id, notification_type, title, 
                        message, is_read, created_at
                 FROM notifications 
                 WHERE user_id = ${user_id}
                 ORDER BY created_at DESC LIMIT 50`
            );
        }

        return from Notification n in notificationStream select n;
    }

    resource function get notifications/count(int user_id) returns json|error {
        stream<record {int unread_count;}, error?> countStream = db->query(
            `SELECT COUNT(*) as unread_count 
             FROM notifications 
             WHERE user_id = ${user_id} AND is_read = FALSE`
        );

        record {|record {int unread_count;} value;|}? result = check countStream.next();
        check countStream.close();

        int unreadCount = result is record {|record {int unread_count;} value;|} ? 
            result.value.unread_count : 0;

        return {"unread_count": unreadCount};
    }

    // ==================== MARK AS READ ====================

    resource function put notifications/[int id]/read() returns http:Response|error {
        http:Response resp = new;

        var result = db->execute(
            `UPDATE notifications SET is_read = TRUE WHERE notification_id = ${id}`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        resp.statusCode = 200;
        resp.setJsonPayload({"message": "Notification marked as read"});
        return resp;
    }

    resource function put notifications/user/[int userId]/read_all() returns http:Response|error {
        http:Response resp = new;

        var result = db->execute(
            `UPDATE notifications SET is_read = TRUE WHERE user_id = ${userId}`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        resp.statusCode = 200;
        resp.setJsonPayload({"message": "All notifications marked as read"});
        return resp;
    }

    // ==================== NOTIFICATION PREFERENCES ====================

    resource function get preferences(int user_id) returns json|http:NotFound|error {
        stream<NotificationPreference, error?> prefStream = db->query(
            `SELECT preference_id, user_id, email_notifications, 
                    sms_notifications, push_notifications
             FROM notification_preferences WHERE user_id = ${user_id}`
        );

        record {|NotificationPreference value;|}|error? result = prefStream.next();
        check prefStream.close();

        if result is record {|NotificationPreference value;|} {
            return result.value.toJson();
        }
        return <http:NotFound>{body: "Preferences not found"};
    }

    resource function post preferences(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        int userId = check req.user_id;
        boolean emailNotif = req.hasKey("email_notifications") ? 
            check req.email_notifications : true;
        boolean smsNotif = req.hasKey("sms_notifications") ? 
            check req.sms_notifications : false;
        boolean pushNotif = req.hasKey("push_notifications") ? 
            check req.push_notifications : true;

        _ = check db->execute(
            `INSERT INTO notification_preferences 
             (user_id, email_notifications, sms_notifications, push_notifications)
             VALUES (${userId}, ${emailNotif}, ${smsNotif}, ${pushNotif})
             ON DUPLICATE KEY UPDATE 
             email_notifications = ${emailNotif},
             sms_notifications = ${smsNotif},
             push_notifications = ${pushNotif}`
        );

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Preferences saved successfully"});
        log:printInfo("Notification preferences updated", user_id = userId);
        return resp;
    }
}

// ==================== KAFKA CONSUMER ====================

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "notification-service-group",
    topics: ["ticket.events", "payment.events", "route.events", "schedule.events"],
    offsetReset: "earliest",
    autoCommit: true
};

service on new kafka:Listener(kafkaBootstrap, consumerConfig) {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            string message = check string:fromBytes(kafkaRecord.value);
            json event = check message.fromJsonString();
            
            map<json> eventMap = check event.ensureType();
            string eventType = check eventMap.event_type;
            
            match eventType {
                "ticket.created" => {
                    check handleTicketCreated(eventMap);
                }
                "ticket.validated" => {
                    check handleTicketValidated(eventMap);
                }
                "ticket.cancelled" => {
                    check handleTicketCancelled(eventMap);
                }
                "payment.completed" => {
                    check handlePaymentCompleted(eventMap);
                }
                "route.created" => {
                    check handleRouteCreated(eventMap);
                }
                "schedule.disruption" => {
                    check handleScheduleDisruption(eventMap);
                }
                _ => {
                    log:printDebug("Unhandled event type", event_type = eventType);
                }
            }
        }
    }
}

function handleTicketCreated(map<json> event) returns error? {
    int userId = check event.user_id;
    string ticketNumber = check event.ticket_number;
    
    _ = check db->execute(
        `INSERT INTO notifications (user_id, notification_type, title, message, sent_at)
         VALUES (${userId}, 'booking_confirmation', 
                'Ticket Created', 
                ${string `Your ticket ${ticketNumber} has been created successfully. Please proceed with payment.`},
                NOW())`
    );
    
    log:printInfo("Notification created for ticket", ticket_number = ticketNumber);
}

function handleTicketValidated(map<json> event) returns error? {
    string ticketNumber = check event.ticket_number;
    
    stream<record {int user_id;}, error?> userStream = db->query(
        `SELECT user_id FROM notifications WHERE message LIKE ${string `%${ticketNumber}%`} LIMIT 1`
    );
    
    record {|record {int user_id;} value;|}|error? result = userStream.next();
    check userStream.close();
    
    if result is record {|record {int user_id;} value;|} {
        int userId = result.value.user_id;
        _ = check db->execute(
            `INSERT INTO notifications (user_id, notification_type, title, message, sent_at)
             VALUES (${userId}, 'ticket_validation', 
                    'Ticket Validated', 
                    ${string `Your ticket ${ticketNumber} has been validated for travel.`},
                    NOW())`
        );
        
        log:printInfo("Validation notification sent", ticket_number = ticketNumber);
    }
}

function handleTicketCancelled(map<json> event) returns error? {
    int userId = check event.user_id;
    string ticketNumber = check event.ticket_number;
    
    _ = check db->execute(
        `INSERT INTO notifications (user_id, notification_type, title, message, sent_at)
         VALUES (${userId}, 'cancellation', 
                'Ticket Cancelled', 
                ${string `Your ticket ${ticketNumber} has been cancelled successfully.`},
                NOW())`
    );
    
    log:printInfo("Cancellation notification sent", ticket_number = ticketNumber);
}

function handlePaymentCompleted(map<json> event) returns error? {
    int userId = check event.user_id;
    string ticketNumber = check event.ticket_number;
    string transactionRef = check event.transaction_reference;
    
    _ = check db->execute(
        `INSERT INTO notifications (user_id, notification_type, title, message, sent_at)
         VALUES (${userId}, 'payment_status', 
                'Payment Successful', 
                ${string `Payment completed for ticket ${ticketNumber}. Transaction: ${transactionRef}`},
                NOW())`
    );
    
    log:printInfo("Payment notification sent", transaction_ref = transactionRef);
}

function handleRouteCreated(map<json> event) returns error? {
    string routeName = check event.route_name;
    
    log:printInfo("New route created notification", route = routeName);
}

function handleScheduleDisruption(map<json> event) returns error? {
    int scheduleId = check event.schedule_id;
    string disruptionType = check event.disruption_type;
    string reason = check event.reason;
    
    log:printInfo("Schedule disruption notification", schedule_id = scheduleId, 'type = disruptionType);
}