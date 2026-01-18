from setuptools import setup, find_packages

setup(
    name="medical-telegram-warehouse",
    version="0.1.0",
    description="Telegram scraper for medical data",
    author="Kara Solutions",
    packages=find_packages(),
    install_requires=[
        "telethon>=1.34.0",
        "python-dotenv>=1.0.0",
        "aiofiles>=23.2.1",
    ],
)