-- ========================================
-- DATABASE: dsa_users
-- Owned by: Passenger Service
-- ========================================
CREATE DATABASE IF NOT EXISTS dsa_users;
USE dsa_users;

CREATE TABLE users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    user_type ENUM('admin', 'client') NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Reviews can stay (user-owned content)
CREATE TABLE reviews (
    review_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL, 
    rating DECIMAL(2,1) CHECK (rating >= 0 AND rating <= 5),
    review_text TEXT,
    route_id INT NOT NULL,
    transport_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ========================================
-- DATABASE: dsa_notifications
-- Owned by: Notification Service
-- ========================================
CREATE DATABASE IF NOT EXISTS dsa_notifications;
USE dsa_notifications;

CREATE TABLE notifications (
    notification_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL, 
    notification_type ENUM('trip_change', 'ticket_validation', 'payment_status', 'booking_confirmation', 'cancellation') NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    is_read BOOLEAN DEFAULT FALSE,
    correlation_id VARCHAR(36), 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP NULL
);

CREATE TABLE notification_preferences (
    preference_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL UNIQUE, 
    email_notifications BOOLEAN DEFAULT TRUE,
    sms_notifications BOOLEAN DEFAULT FALSE,
    push_notifications BOOLEAN DEFAULT TRUE
);


-- ========================================
-- DATABASE: dsa_routes
-- Owned by: Transport Service
-- ========================================
CREATE DATABASE IF NOT EXISTS dsa_routes;
USE dsa_routes;

CREATE TABLE transports (
    transport_id INT PRIMARY KEY AUTO_INCREMENT,
    transport_type ENUM('BUS', 'TRAIN') NOT NULL,
    brand VARCHAR(100) NOT NULL,
    registration_number VARCHAR(50) UNIQUE NOT NULL,
    total_capacity INT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transport_classes (
    class_id INT PRIMARY KEY AUTO_INCREMENT,
    transport_id INT NOT NULL,
    class_name VARCHAR(50) NOT NULL,
    seat_count INT NOT NULL,
    price_multiplier DECIMAL(3,2) DEFAULT 1.00
);

CREATE TABLE routes (
    route_id INT PRIMARY KEY AUTO_INCREMENT,
    route_name VARCHAR(255) NOT NULL,
    start_point VARCHAR(255) NOT NULL,
    end_point VARCHAR(255) NOT NULL,
    distance_km DECIMAL(10,2),
    base_price DECIMAL(10,2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE schedules (
    schedule_id INT PRIMARY KEY AUTO_INCREMENT,
    route_id INT NOT NULL, 
    transport_id INT NOT NULL,
    departure_time TIME NOT NULL,
    arrival_time TIME NOT NULL,
    operating_days VARCHAR(50) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (route_id) REFERENCES routes(route_id),
    FOREIGN KEY (transport_id) REFERENCES transports(transport_id)
);


-- ========================================
-- DATABASE: dsa_tickets
-- Owned by: Ticketing Service (renamed from dsa_bookings)
-- ========================================
CREATE DATABASE IF NOT EXISTS dsa_tickets;
USE dsa_tickets;

CREATE TABLE tickets (
    ticket_id INT PRIMARY KEY AUTO_INCREMENT,
    ticket_number VARCHAR(50) UNIQUE NOT NULL, 
    user_id INT NOT NULL, 
    route_id INT NOT NULL, 
    transport_id INT NOT NULL,
    schedule_id INT NOT NULL,
    travel_date DATE NOT NULL,
    ticket_type ENUM('single', 'return', 'day_pass', 'weekly') NOT NULL, 
    class_id INT NOT NULL, 
    number_of_passengers INT NOT NULL DEFAULT 1, 
    total_price DECIMAL(10,2) NOT NULL,
    status ENUM('created', 'paid', 'validated', 'expired', 'cancelled') DEFAULT 'created',
    payment_id INT NULL, 
    correlation_id VARCHAR(36),
    validated_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    expiry_time TIMESTAMP NOT NULL
);

CREATE TABLE reserved_seats (
    id INT PRIMARY KEY AUTO_INCREMENT,
    ticket_id INT NOT NULL,
    seat_number VARCHAR(10) NOT NULL,
    class_id INT NOT NULL,
    UNIQUE KEY unique_seat (ticket_id, seat_number)
);


-- ========================================
-- DATABASE: dsa_payments
-- Owned by: Payment Service
-- ========================================
CREATE DATABASE IF NOT EXISTS dsa_payments;
USE dsa_payments;

CREATE TABLE payments (
    payment_id INT PRIMARY KEY AUTO_INCREMENT,
    ticket_id INT NOT NULL, 
    user_id INT NOT NULL, 
    amount DECIMAL(10,2) NOT NULL,
    payment_type ENUM('cash', 'card', 'mobile_money', 'bank_transfer') NOT NULL,
    payment_status ENUM('paid', 'pending', 'expired', 'failed', 'refunded') DEFAULT 'pending',
    transaction_reference VARCHAR(100) UNIQUE NOT NULL, 
    correlation_id VARCHAR(36),
    payment_date TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE payment_methods (
    method_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL, 
    payment_type ENUM('card', 'mobile_money', 'bank_transfer') NOT NULL,
    card_last_four VARCHAR(4),
    card_brand VARCHAR(50),
    mobile_number VARCHAR(20),
    is_default BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE refunds (
    refund_id INT PRIMARY KEY AUTO_INCREMENT,
    payment_id INT NOT NULL,
    ticket_id INT NOT NULL,
    refund_amount DECIMAL(10,2) NOT NULL,
    refund_reason TEXT,
    refund_status ENUM('pending', 'processed', 'rejected') DEFAULT 'pending',
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL
);
