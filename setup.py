from distutils.core import setup

setup(
    name='pycps',
    version='0.1.0',
    author='Viesturs Silins',
    author_email='example@example.com',
    packages=['pycps', 'pycps.test'],
    scripts=['bin/run_server'],
    url='http://example.com/',
    license='LICENSE.txt',
    description='',
    long_description=open('README.txt').read(),
    install_requires=[],
)
