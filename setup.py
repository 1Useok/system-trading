from pathlib import Path

from setuptools import find_packages, setup

this_dir = Path(__file__).parent

setup(
    name="trading_system",
    version="1.0.0",
    description="Event-driven futures trading system on the KIS (Korea Investment & Securities) Open API",
    long_description=(this_dir / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests", "tests.*")),
    include_package_data=True,
    package_data={"trading_system": ["config/*.yaml"]},
    python_requires=">=3.9,<3.12",
    install_requires=[
        "pandas>=1.5.3,<2.0.0",
        "numpy>=1.24.3,<2.0.0",
        "aiohttp>=3.8.0",
        "PyYAML>=6.0.0",
        "python-dotenv>=0.19.0",
        "websockets>=10.0.0",
        "pycryptodome>=3.18.0",
    ],
)
