from setuptools import setup, find_packages

requires = [
    'ptah',
    'pyramid',
    'gevent',
    'haigha',
    'redis',
]

setup(
    name='smxq',
    version='1.0dev',
    description='smxq lib',
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Pylons",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
    url='https://github.com/socialmaniacs/cs2',
    packages=find_packages(),
    zip_safe=False,
    install_requires = requires,
    include_package_data=True,
    entry_points = {
        'console_scripts': [
            'smxq-backend = smxq.start:main',
            ],
        'paste.filter_app_factory': [
            'dispatcher = smxq:StartDispatcher',
            ]
        },
    )
