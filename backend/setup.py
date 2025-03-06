from setuptools import setup, find_packages

setup(
    name="FinalTennisBot",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'fastapi',
        'uvicorn',
        'aiohttp',
        'pytz',
        'rapidfuzz'
    ],
)