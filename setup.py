from setuptools import find_packages, setup

setup(
    name="masto65ezfd86424f69a",
    version="0.0.6",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "aiohttp",
        "beautifulsoup4>=4.11",
        "python_dateutil>=2.8"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
