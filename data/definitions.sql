CREATE TABLE IF NOT EXISTS payment_method (
    payment_method_id SERIAL PRIMARY KEY,
    payment_method VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS frequency (
    frequency_id SERIAL PRIMARY KEY,
    frequency_name VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS customer (
    customer_id SERIAL PRIMARY KEY,
    age INT NOT NULL,
    gender ENUM('Male', 'Female') NOT NULL,
    location_id BIGINT UNSIGNED NOT NULL,
    subscription_status ENUM('Yes', 'No') NOT NULL,
    previous_purchases INT DEFAULT 0,
    preferred_payment_method_id BIGINT UNSIGNED NOT NULL,
    frequency_of_purchases_id BIGINT UNSIGNED NOT NULL,
    FOREIGN KEY (preferred_payment_method_id) REFERENCES payment_method(payment_method_id),
    FOREIGN KEY (frequency_of_purchases_id) REFERENCES frequency(frequency_id),
    FOREIGN KEY (location_id) REFERENCES location(location_id)
);

CREATE TABLE IF NOT EXISTS category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS item (
    item_id SERIAL PRIMARY KEY,
    item_purchased VARCHAR(16) NOT NULL,
    category_id BIGINT UNSIGNED NOT NULL,
    size VARCHAR(4) NOT NULL,
    color VARCHAR(16) NOT NULL,
    FOREIGN KEY (category_id) REFERENCES category(category_id)
);

CREATE TABLE IF NOT EXISTS season (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS shipping_type (
    shipping_type_id SERIAL PRIMARY KEY,
    shipping_type_name VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS purchase (
    purchase_id SERIAL PRIMARY KEY,
    item_id BIGINT UNSIGNED NOT NULL,
    customer_id BIGINT UNSIGNED NOT NULL,
    purchase_amount DECIMAL(10, 2) NOT NULL,
    season_id BIGINT UNSIGNED NOT NULL,
    review_rating DECIMAL(2, 1) CHECK (review_rating >= 0 AND review_rating <= 5),
    payment_method_id BIGINT UNSIGNED NOT NULL,
    shipping_type_id BIGINT UNSIGNED NOT NULL,
    discount_applied ENUM('Yes', 'No') NOT NULL,
    promo_code_used ENUM('Yes', 'No') NOT NULL,
    FOREIGN KEY (item_id) REFERENCES item(item_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (season_id) REFERENCES season(season_id),
    FOREIGN KEY (payment_method_id) REFERENCES payment_method(payment_method_id),
    FOREIGN KEY (shipping_type_id) REFERENCES shipping_type(shipping_type_id)
);

