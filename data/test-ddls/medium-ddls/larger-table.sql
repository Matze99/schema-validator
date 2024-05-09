-- Standard Alter Table SQL

ALTER TABLE example_db.user
    DROP CONSTRAINT user_pkey

;

-- Drop Constraint, Rename and Create Table SQL

ALTER TABLE example_db.user_info
    RENAME TO user_a1fb8f80

;

ALTER TABLE example_db.user_info ADD Example INT NOT NULL;

CREATE TABLE example_db.user_info
(
    user_id                      varchar(32)   NOT NULL,
    user_code                    varchar(100),
    user_name                    varchar(100),
    user_start_date              timestamp,
    role_type                        varchar(50),
    english_name                     varchar(100),
    status                           varchar(50),
    dunn_bradstreet_number           varchar(100),
    user_risk_status             varchar(50),
    user_risk_status_description varchar(255),
    user_account_group           varchar(50),
    billing_code                     varchar(100),
    billing_code_description         varchar(255),
    friend_id               varchar(32),
    default_location_address_id      varchar(32)   NOT NULL,
    meta_record_type             varchar(27),
    meta_record_timestamp        timestamp,
    meta_source_system_id        varchar(50),
    meta_user_updated            varchar(50),
    meta_batch_id                varchar(27),
    meta_start_timestamp         timestamp,
    meta_end_timestamp           timestamp,
    meta_active_record           varchar(1),
--    PRIMARY KEY (user_id),
    FOREIGN KEY (default_location_address_id) REFERENCES "materials" ("id")
)

;

ALTER TABLE "example_db"."user_info" UPDATE meta_active_record INT NOT NULL;

ALTER TABLE example_db.user_info ADD FOREIGN KEY (default_location_address_id) REFERENCES "materials" ("id");

ALTER TABLE example_db.user_info
    ADD CONSTRAINT user_id_pkey PRIMARY KEY (user_id);