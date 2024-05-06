from faker import Faker

# Crear una instancia de Faker
fake = Faker()

# Generar un nombre falso
fake_name = fake.name()
print(fake_name)

# Generar una dirección falsa
fake_address = fake.address()
print(fake_address)

# Generar un número de teléfono falso
fake_phone_number = fake.phone_number()
print(fake_phone_number)

# https://faker.readthedocs.io/en/master/
    # Direcciones de correo electrónico: fake.email()
    # Texto aleatorio: fake.text()
    # Fechas: fake.date(), fake.date_of_birth(), fake.future_date(), fake.past_date()
    # Nombres de empresas: fake.company()
    # Tarjetas de crédito: fake.credit_card_number()
    # Nombres de usuarios: fake.user_name()
    # Contraseñas: fake.password()
    # Números de seguridad social: fake.ssn()
    # URLs: fake.url()
    # Colores: fake.color_name()
    # Palabras y frases: fake.word(), fake.sentence()


import random

# Lista de opciones
options = ['rojo', 'azul', 'verde', 'amarillo', 'naranja']

# Elegir un color aleatorio
random_color = random.choice(options)
print(random_color)