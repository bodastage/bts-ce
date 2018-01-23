from setuptools import setup

setup(
    name='btsapi',
    packages=['btsapi'],
    include_package_data=True,
    install_requires=[
        'flask',
        'flask-sqlalchemy',
        'flask-jsontools',
        'flask-marshmallow',
        'marshmallow-sqlalchemy',
        'flask-bcrypt',
        'flask-testing',
        'flask-cors',
        'sqlalchemy-datatables',
        'flask-login',
        'blinker'
    ],
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest',
    ],
)