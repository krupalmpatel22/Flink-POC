from faker import Faker
import random
faker = Faker()

print(faker.currency_code(), random.randint(1, 100))