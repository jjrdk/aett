from setuptools import setup, find_packages

setup(
    name='your_package_name',
    version='0.1.0',
    packages=find_packages(),
    author='Jacob Reimers',
    author_email='openmedstack@openmedstack.org',
    description='OpenMedStack event store client for Python',
    long_description=open('aett_eventstore/README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/openmedstack/neventstore',
    license='MIT',
    install_requires=open('requirements.txt').readlines(),
    classifiers=[
        'Programming Language :: Python :: 3',
        # Add more classifiers as needed
    ],
)
