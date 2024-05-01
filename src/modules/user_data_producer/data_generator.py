from faker import Faker
import uuid
import json

class DataGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate(self):
        return {
            'user_id': str(uuid.uuid4()), 
            'first_name': self.fake.first_name(),
            'last_name': self.fake.last_name(),
            'email': self.fake.email(),
            'phone_number': self.fake.phone_number(),
            'address': self.fake.address(),
            'city': self.fake.city(),
            'state': self.fake.state(),
            'country': self.fake.country(),
            'zip_code': self.fake.zipcode(),
            'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),  # Assuming random date of birth between 18 and 90 years old
            'gender': self.fake.random_element(elements=('Male', 'Female', 'Other')),
            'occupation': self.fake.job(),
            'company_name': self.fake.company(),
            'website': self.fake.url(),
            'account_balance': self.fake.random_number(digits=5) + self.fake.random_number(digits=2) / 100,  # Assuming random decimal account balance
            'credit_score': self.fake.random_int(min=300, max=850),  # Assuming random credit score
            'registration_date': self.fake.date_this_year(before_today=True, after_today=False).isoformat(),  # Assuming random registration date within this year
            'last_login': self.fake.date_time_this_month(before_now=True, after_now=False).isoformat(),  # Assuming random last login date within this month
            'subscription_status': self.fake.random_element(elements=('Active', 'Inactive')),
            'subscription_type': self.fake.random_element(elements=('Basic', 'Premium', 'Enterprise')),
            'profile_picture_url': self.fake.image_url(),
            'social_security_number': self.fake.ssn(),
            'passport_number': self.fake.ssn(),
            'driver_license_number': self.fake.ssn(),
            'marital_status': self.fake.random_element(elements=('Single', 'Married', 'Divorced', 'Widowed')),
            'children_count': self.fake.random_int(min=0, max=5),  # Assuming random number of children between 0 and 5
            'emergency_contact_name': self.fake.name(),
            'emergency_contact_number': self.fake.phone_number(),
            'medical_conditions': self.fake.text(),
            'blood_type': self.fake.random_element(elements=('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')),
            'allergies': self.fake.text(),
            'favorite_color': self.fake.color_name(),
            'favorite_food': self.fake.word(ext_word_list=['pizza', 'sushi', 'burger', 'pasta', 'salad']),
            'favorite_movie': self.fake.word(ext_word_list=[
                    'The Shawshank Redemption', 'The Godfather', 'The Dark Knight', 'Pulp Fiction', 'Fight Club',
                    'Forrest Gump', 'The Matrix', 'Goodfellas', 'Schindler\'s List', 'The Lord of the Rings: The Return of the King',
                    'The Lord of the Rings: The Fellowship of the Ring', 'The Lord of the Rings: The Two Towers', 'Inception',
                    'The Silence of the Lambs', 'The Green Mile', 'The Lion King', 'Gladiator', 'The Departed',
                    'The Godfather: Part II', 'The Prestige', 'The Usual Suspects', 'Se7en', 'Interstellar',
                    'Saving Private Ryan', 'The Pianist', 'The Shawshank Redemption', 'The Dark Knight Rises', 'The Avengers',
                    'The Wolf of Wall Street', 'Inglourious Basterds', 'The Sixth Sense', 'The Social Network', 'Avatar',
                    'The Pursuit of Happyness', 'The Revenant', 'Catch Me If You Can', 'The Curious Case of Benjamin Button',
                    'The Departed', 'The Bourne Identity', 'The Bourne Supremacy', 'The Bourne Ultimatum', 'Titanic'
                ]),
            'favorite_sport': self.fake.word(ext_word_list=['Football', 'Basketball', 'Baseball', 'Soccer', 'Tennis', 'Golf', 'Cricket', 'Rugby', 'Volleyball', 'Hockey']),  # Manually providing a list of sports names
            'hobby': self.fake.word(ext_word_list=['Reading', 'Painting', 'Cooking', 'Photography', 'Gardening', 'Writing', 'Fishing', 'Hiking', 'Dancing', 'Knitting']),  # Manually providing a list of hobby names
            'pet_name': self.fake.first_name(),
            'pet_type': self.fake.random_element(elements=('Dog', 'Cat', 'Bird', 'Fish', 'Other')),
            'pet_age': self.fake.random_int(min=0, max=20),  # Assuming random age of pet between 0 and 20 years
        }

def generate_data():
    data_generator = DataGenerator()
    data = data_generator.generate()
    print(data)

def generate_data_file():
    data_generator = DataGenerator()
    json_list = []
    for i in range(20):
        data = data_generator.generate()
        json_list.append(data)
    file_path = 'data.json'
    with open(file_path, 'w') as json_file:
        json.dump(json_list, json_file)
    print(data)

if __name__ == '__main__':
    #generate_data_file()
    generate_data()