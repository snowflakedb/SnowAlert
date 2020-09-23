from setuptools import setup, find_packages

setup(
    name="webui",
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'flask',
        'flask-bcrypt',
        'flask-migrate',
        'flask-script',
        'flask-sqlalchemy',
        'gunicorn',
        'itsdangerous',
        'logbook',
        'pendulum',
        'requests',
        'ujson',
    ],
)
