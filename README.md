# Flow Diagram

![Flow Diagram](https://drive.google.com/file/d/1QjVeivnsq_UmdI_hgPRd-6TBvOzyr6KE/view)

## Module Description

| Module Name    | Working Description                                                                                   |
|----------------|-------------------------------------------------------------------------------------------------------|
| Data Producer  | Generates data using a Python script that utilizes the Faker package to generate fake data every 5-10 minutes and then sends this data to a Kafka topic. |
| Kafka          | Operates as a distributed streaming platform, where producers publish data to topics, and consumers subscribe to receive messages for real-time data processing. |
| Data Processor | Fetches data from a Kafka topic, then validates, processes, and finally stores it into a MySQL database using Flink. |
| MySQL          | This is a database where the processed data will be stored.                                         |

## Data Schema

| Field Name            | Description                                    | Data Type    |
|-----------------------|------------------------------------------------|--------------|
| user_id               | Unique identifier for each user                | INT          |
| first_name            | First name of the user                         | VARCHAR(50)  |
| last_name             | Last name of the user                          | VARCHAR(50)  |
| email                 | Email address of the user                      | VARCHAR(100) |
| phone_number          | Phone number of the user                       | VARCHAR(20)  |
| address               | Address of the user                            | VARCHAR(100) |
| city                  | City of the user                               | VARCHAR(50)  |
| state                 | State of the user                              | VARCHAR(50)  |
| country               | Country of the user                            | VARCHAR(50)  |
| zip_code              | ZIP code of the user                           | VARCHAR(20)  |
| date_of_birth         | Date of birth of the user                      | DATE         |
| gender                | Gender of the user (Male, Female, Other)       | ENUM         |
| nationality           | Nationality of the user                        | VARCHAR(50)  |
| occupation            | Occupation of the user                         | VARCHAR(50)  |
| company_name          | Name of the company the user works for         | VARCHAR(100) |
| website               | Personal or professional website URL of the user | VARCHAR(100) |
| account_balance       | Account balance of the user                    | DECIMAL(10, 2) |
| credit_score          | Credit score of the user                       | INT          |
| registration_date     | Date when the user registered                  | DATE         |
| last_login            | Date and time of the user's last login         | DATETIME     |
| subscription_status   | Status of user's subscription (Active, Inactive) | ENUM       |
| subscription_type     | Type of user's subscription (Basic, Premium, Enterprise) | ENUM |
| profile_picture_url   | URL of the user's profile picture              | VARCHAR(200) |
| social_security_number| Social security number of the user             | VARCHAR(20)  |
| passport_number       | Passport number of the user                    | VARCHAR(20)  |
| driver_license_number | Driver's license number of the user            | VARCHAR(20)  |
| marital_status        | Marital status of the user (Single, Married, Divorced, Widowed) | ENUM |
| children_count        | Number of children the user has                | INT          |
| emergency_contact_name| Name of the user's emergency contact           | VARCHAR(100) |
| emergency_contact_number| Phone number of the user's emergency contact | VARCHAR(20) |
| medical_conditions    | Any medical conditions the user has            | TEXT         |
| blood_type            | Blood type of the user                         | ENUM         |
| allergies             | Any allergies the user has                     | TEXT         |
| favorite_color        | User's favorite color                          | VARCHAR(20)  |
| favorite_food         | User's favorite food                           | VARCHAR(50)  |
| favorite_movie        | User's favorite movie                          | VARCHAR(100) |
| favorite_music_genre  | User's favorite music genre                    | VARCHAR(50)  |
| favorite_sport        | User's favorite sport                          | VARCHAR(50)  |
| hobby                 | User's hobby                                   | VARCHAR(100) |
| pet_name              | Name of the user's pet                         | VARCHAR(50)  |
| pet_type              | Type of the user's pet (Dog, Cat, Bird, Fish, Other) | ENUM   |
| pet_breed             | Breed of the user's pet                        | VARCHAR(50)  |
| pet_age               | Age of the user's pet                          | INT          |
| vehicle_make          | Make of the user's vehicle                     | VARCHAR(50)  |
| vehicle_model         | Model of the user's vehicle                    | VARCHAR(50)  |
| vehicle_year          | Year of the user's vehicle                     | INT          |
| license_plate_number  | License plate number of the user's vehicle     | VARCHAR(20)  |
| insurance_provider    | Insurance provider of the user                 | VARCHAR(100) |
| insurance_policy_number| Policy number of the user's insurance policy   | VARCHAR(50) |

